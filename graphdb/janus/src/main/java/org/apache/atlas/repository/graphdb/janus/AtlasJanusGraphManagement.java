/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import com.google.common.base.Preconditions;
import jnr.a64asm.REG;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.core.schema.*;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.internal.Token;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.hadoop.MapReduceIndexManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.core.schema.SchemaAction.DISABLE_INDEX;
import static org.janusgraph.core.schema.SchemaAction.ENABLE_INDEX;
import static org.janusgraph.core.schema.SchemaAction.REGISTER_INDEX;
import static org.janusgraph.core.schema.SchemaAction.REMOVE_INDEX;
import static org.janusgraph.core.schema.SchemaStatus.*;

/**
 * Janus implementation of AtlasGraphManagement.
 */
public class AtlasJanusGraphManagement implements AtlasGraphManagement {
    private static final boolean lockEnabled = AtlasConfiguration.STORAGE_CONSISTENCY_LOCK_ENABLED.getBoolean();
    private static final Parameter[] STRING_PARAMETER_ARRAY = new Parameter[]{Mapping.STRING.asParameter()};

    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphManagement.class);
    private static final char[] RESERVED_CHARS = {'{', '}', '"', '$', Token.SEPARATOR_CHAR};
    private String ES_SEARCH_FIELD_KEY = "search";
    private String ES_FILTER_FIELD_KEY = "filter";

    private AtlasJanusGraph graph;
    private JanusGraphManagement management;
    private Set<String> newMultProperties = new HashSet<>();

    public AtlasJanusGraphManagement(AtlasJanusGraph graph, JanusGraphManagement managementSystem) {
        this.management = managementSystem;
        this.graph = graph;
    }

    @Override
    public void createVertexMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        IndexBuilder indexBuilder = management.buildIndex(indexName, Vertex.class);

        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey);
        }

        indexBuilder.buildMixedIndex(backingIndex);
    }

    @Override
    public void createEdgeMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        IndexBuilder indexBuilder = management.buildIndex(indexName, Edge.class);

        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey);
        }

        indexBuilder.buildMixedIndex(backingIndex);
    }

    @Override
    public void createEdgeIndex(String label, String indexName, AtlasEdgeDirection edgeDirection, List<AtlasPropertyKey> propertyKeys) {
        EdgeLabel edgeLabel = management.getEdgeLabel(label);

        if (edgeLabel == null) {
            edgeLabel = management.makeEdgeLabel(label).make();
        }

        Direction direction = AtlasJanusObjectFactory.createDirection(edgeDirection);
        PropertyKey[] keys = AtlasJanusObjectFactory.createPropertyKeys(propertyKeys);

        if (management.getRelationIndex(edgeLabel, indexName) == null) {
            management.buildEdgeIndex(edgeLabel, indexName, direction, keys);
        }
    }

    @Override
    public void createFullTextMixedIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        IndexBuilder indexBuilder = management.buildIndex(indexName, Vertex.class);

        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey, org.janusgraph.core.schema.Parameter.of("mapping", Mapping.TEXT));
        }

        indexBuilder.buildMixedIndex(backingIndex);
    }

    @Override
    public boolean containsPropertyKey(String propertyName) {
        return management.containsPropertyKey(propertyName);
    }

    @Override
    public void rollback() {
        management.rollback();

    }

    @Override
    public void commit() {
        graph.addMultiProperties(newMultProperties);
        newMultProperties.clear();
        management.commit();
    }

    private static void checkName(String name) {
        //for some reason, name checking was removed from StandardPropertyKeyMaker.make()
        //in Janus.  For consistency, do the check here.
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");

        for (char c : RESERVED_CHARS) {
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
        }

    }

    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        if (cardinality.isMany()) {
            newMultProperties.add(propertyName);
        }

        PropertyKeyMaker propertyKeyBuilder = management.makePropertyKey(propertyName).dataType(propertyClass);
        if (cardinality != null) {
            Cardinality janusCardinality = AtlasJanusObjectFactory.createCardinality(cardinality);
            propertyKeyBuilder.cardinality(janusCardinality);
        }

        PropertyKey propertyKey = propertyKeyBuilder.make();

        return GraphDbObjectFactory.createPropertyKey(propertyKey);
    }

    @Override
    public AtlasEdgeLabel makeEdgeLabel(String label) {
        EdgeLabel edgeLabel = management.makeEdgeLabel(label).make();

        return GraphDbObjectFactory.createEdgeLabel(edgeLabel);
    }

    @Override
    public void deletePropertyKey(String propertyKey) {
        PropertyKey janusPropertyKey = management.getPropertyKey(propertyKey);

        if (null == janusPropertyKey) return;

        for (int i = 0; ; i++) {
            String deletedKeyName = janusPropertyKey + "_deleted_" + i;

            if (null == management.getPropertyKey(deletedKeyName)) {
                management.changeName(janusPropertyKey, deletedKeyName);
                break;
            }
        }
    }

    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {
        checkName(propertyName);

        return GraphDbObjectFactory.createPropertyKey(management.getPropertyKey(propertyName));
    }

    @Override
    public AtlasEdgeLabel getEdgeLabel(String label) {
        return GraphDbObjectFactory.createEdgeLabel(management.getEdgeLabel(label));
    }

    @Override
    public String addMixedIndex(String indexName, AtlasPropertyKey propertyKey, boolean isStringField) {
        return addMixedIndex(indexName, propertyKey, isStringField, new HashMap<>(), new HashMap<>());
    }

    @Override
    public String addMixedIndex(String indexName, AtlasPropertyKey propertyKey, boolean isStringField, HashMap<String, Object> indexTypeESConfig, HashMap<String, HashMap<String, Object>> indexTypeESFields) {
        PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(propertyKey);
        JanusGraphIndex janusGraphIndex = management.getGraphIndex(indexName);

        ArrayList<Parameter> params = new ArrayList<>();

        if (isStringField) {
            params.add(Mapping.STRING.asParameter());
            LOG.debug("string type for {} with janueKey {}.", propertyKey.getName(), janusKey);
        }

        if (MapUtils.isNotEmpty(indexTypeESConfig)) {
            for (String esPropertyKey : indexTypeESConfig.keySet()) {
                Object esPropertyValue = indexTypeESConfig.get(esPropertyKey);
                params.add(Parameter.of(ParameterType.customParameterName(esPropertyKey), esPropertyValue));
            }
        }


        if (MapUtils.isNotEmpty(indexTypeESFields)) {
            params.add(Parameter.of(ParameterType.customParameterName("fields"), indexTypeESFields));
        }

        if (params.size() > 0) {
            management.addIndexKey(janusGraphIndex, janusKey, params.toArray(new Parameter[0]));
        } else {
            management.addIndexKey(janusGraphIndex, janusKey);
        }
        LOG.debug("created a type for {} with janueKey {}.", propertyKey.getName(), janusKey);

        String encodedName = "";
        if (isStringField) {
            encodedName = graph.getIndexFieldName(propertyKey, janusGraphIndex, STRING_PARAMETER_ARRAY);
        } else {
            encodedName = graph.getIndexFieldName(propertyKey, janusGraphIndex);
        }


        LOG.info("property '{}' is encoded to '{}'.", propertyKey.getName(), encodedName);

        return encodedName;
    }

    @Override
    public String getIndexFieldName(String indexName, AtlasPropertyKey propertyKey, boolean isStringField) {
        JanusGraphIndex janusGraphIndex = management.getGraphIndex(indexName);

        if (isStringField) {
            return graph.getIndexFieldName(propertyKey, janusGraphIndex, STRING_PARAMETER_ARRAY);
        } else {
            return graph.getIndexFieldName(propertyKey, janusGraphIndex);
        }

    }

    public AtlasGraphIndex getGraphIndex(String indexName) {
        JanusGraphIndex index = management.getGraphIndex(indexName);

        return GraphDbObjectFactory.createGraphIndex(index);
    }

    @Override
    public boolean edgeIndexExist(String label, String indexName) {
        EdgeLabel edgeLabel = management.getEdgeLabel(label);

        return edgeLabel != null && management.getRelationIndex(edgeLabel, indexName) != null;
    }

    @Override
    public void createVertexCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        createCompositeIndex(propertyName, isUnique, propertyKeys, Vertex.class);
    }

    @Override
    public void createEdgeCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {
        createCompositeIndex(propertyName, isUnique, propertyKeys, Edge.class);
    }

    private void createCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys, Class<? extends Element> elementType) {
        IndexBuilder indexBuilder = management.buildIndex(propertyName, elementType);

        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey);
        }

        if (isUnique) {
            indexBuilder.unique();
        }

        JanusGraphIndex index = indexBuilder.buildCompositeIndex();

        if (lockEnabled && isUnique) {
            management.setConsistency(index, ConsistencyModifier.LOCK);
        }
    }

    @Override
    public void updateUniqueIndexesForConsistencyLock() {
        try {
            setConsistency(this.management, Vertex.class);
            setConsistency(this.management, Edge.class);
        } finally {
            commit();
        }
    }

    @Override
    public void enableIndex() {
        enableIndex(this.management, this.graph, Vertex.class);
        enableIndex(this.management, this.graph, Edge.class);
    }

    @Override
    public void enableIndexForTypeSync() {
        enableIndex(this.management, this.graph, ActiveIndexNameManager.getCurrentWriteVertexIndexName());
    }

    @Override
    public void disableIndex(String indexName) {
        AtlasJanusGraph graph = this.graph;
        JanusGraphManagement management = this.management;

        JanusGraphIndex index = management.getGraphIndex(indexName);

        PropertyKey[] propertyKeys = index.getFieldKeys();
        SchemaStatus status = index.getIndexStatus(propertyKeys[0]);

        try {
            LOG.info("schemastatus is {}", status);

            if (status != DISABLED) {
                updateIndexStatus(graph, indexName, DISABLE_INDEX, status, DISABLED);
            }
        } catch (InterruptedException e) {
            LOG.error("IllegalStateException for indexName : {}, Exception: ", indexName, e);
        } catch (ExecutionException e) {
            LOG.error("ExecutionException for indexName : {}, Exception: ", indexName, e);
        }
    }

    @Override
    public void removeIndex(String indexName) {
        StandardJanusGraph graph = (StandardJanusGraph) this.graph.getGraph();
        JanusGraphManagement management = graph.openManagement();

        JanusGraphIndex index = management.getGraphIndex(indexName);
        PropertyKey[] propertyKeys = index.getFieldKeys();
        SchemaStatus status = index.getIndexStatus(propertyKeys[0]);

        try {
            if (status != DISABLED) {
                graph.getOpenTransactions().forEach(tx -> graph.closeTransaction((StandardJanusGraphTx) tx));

                MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);

                JanusGraphManagement.IndexJobFuture future = mr.updateIndex(index, SchemaAction.REMOVE_INDEX);

                management.commit();
                //graph.tx().commit();
                future.get();
            }
        } catch (BackendException e) {
            LOG.error("BackendException for indexName : {}, Exception: ", indexName, e);
        } catch (InterruptedException e) {
            LOG.error("IllegalStateException for indexName : {}, Exception: ", indexName, e);
        } catch (ExecutionException e) {
            LOG.error("ExecutionException for indexName : {}, Exception: ", indexName, e);
        }
    }

    private static void enableIndex(JanusGraphManagement mgmt, AtlasJanusGraph graph,
                                          String indexName) {
        LOG.info("updating SchemaStatus after typeSync for index {}: Starting...", indexName);

        JanusGraphIndex index = mgmt.getGraphIndex(indexName);
        int count;

        if (index != null) {
            LOG.info("isCompositeIndex: {}", index.isCompositeIndex());
            LOG.info("isMixedIndex: {}", index.isMixedIndex());

            count = enableIndex(graph, index, true);

            LOG.info("updated SchemaStatus for index {}: {}: Done!", index, count);
        } else {
            LOG.error("index with name {} not found while attempting to Enable index", indexName);
        }
    }

    private static void enableIndex(JanusGraphManagement mgmt, AtlasJanusGraph graph, Class<? extends Element> elementType) {
        LOG.info("updating SchemaStatus for {}: Starting...", elementType.getSimpleName());
        int count = 0;

        Iterable<JanusGraphIndex> iterable = mgmt.getGraphIndexes(elementType);

        for (JanusGraphIndex index : iterable) {

            if (index.isCompositeIndex()) {
                count = enableIndex(graph, index, false);
            }
        }

        LOG.info("updating SchemaStatus for {}: {}: Done!", elementType.getSimpleName(), count);
    }

    private static void setConsistency(JanusGraphManagement mgmt, Class<? extends Element> elementType) {
        LOG.info("setConsistency: {}: Starting...", elementType.getSimpleName());
        int count = 0;

        try {
            Iterable<JanusGraphIndex> iterable = mgmt.getGraphIndexes(elementType);
            for (JanusGraphIndex index : iterable) {
                if (!index.isCompositeIndex() || !index.isUnique() || mgmt.getConsistency(index) == ConsistencyModifier.LOCK) {
                    continue;
                }

                for (PropertyKey propertyKey : index.getFieldKeys()) {
                    LOG.info("setConsistency: {}: {}", count, propertyKey.name());
                }

                mgmt.setConsistency(index, ConsistencyModifier.LOCK);
                count++;
            }
        } finally {
            LOG.info("setConsistency: {}: {}: Done!", elementType.getSimpleName(), count);
        }
    }

    @Override
    public void reindex(String indexName, List<AtlasElement> elements) throws Exception {
        try {
            JanusGraphIndex index = management.getGraphIndex(indexName);
            if (index == null || !(management instanceof ManagementSystem) || !(graph.getGraph() instanceof StandardJanusGraph)) {
                LOG.error("Could not retrieve index for name: {} ", indexName);
                return;
            }

            ManagementSystem managementSystem = (ManagementSystem) management;
            IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
            if (!(indexType instanceof MixedIndexType)) {
                LOG.warn("Index: {}: Not of MixedIndexType ", indexName);
                return;
            }

            IndexSerializer indexSerializer = ((StandardJanusGraph) graph.getGraph()).getIndexSerializer();
            reindexElement(managementSystem, indexSerializer, (MixedIndexType) indexType, elements);
        } catch (Exception exception) {
            throw exception;
        } finally {
            management.commit();
        }
    }

    @Override
    public Object startIndexRecovery(long recoveryStartTime) {
        Instant recoveryStartInstant = Instant.ofEpochMilli(recoveryStartTime);
        JanusGraph janusGraph = this.graph.getGraph();

        return JanusGraphFactory.startTransactionRecovery(janusGraph, recoveryStartInstant);
    }

    @Override
    public void stopIndexRecovery(Object txRecoveryObject) {
        if (txRecoveryObject == null) {
            return;
        }

        try {
            if (txRecoveryObject instanceof TransactionRecovery) {
                TransactionRecovery txRecovery = (TransactionRecovery) txRecoveryObject;
                StandardJanusGraph janusGraph = (StandardJanusGraph) this.graph.getGraph();

                LOG.info("stopIndexRecovery: Index Client is unhealthy. Index recovery: Paused!");

                janusGraph.getBackend().getSystemTxLog().close();

                txRecovery.shutdown();
            } else {
                LOG.error("stopIndexRecovery({}): Invalid transaction recovery object!", txRecoveryObject);
            }
        } catch (Exception e) {
            LOG.warn("stopIndexRecovery: Error while shutting down transaction recovery", e);
        }
    }

    @Override
    public void printIndexRecoveryStats(Object txRecoveryObject) {
        if (txRecoveryObject == null) {
            return;
        }

        try {
            if (txRecoveryObject instanceof TransactionRecovery) {
                StandardTransactionLogProcessor txRecovery = (StandardTransactionLogProcessor) txRecoveryObject;
                long[] statistics = txRecovery.getStatistics();

                if (statistics.length >= 2) {
                    LOG.info("Index Recovery: Stats: Success:{}: Failed: {}", statistics[0], statistics[1]);
                } else {
                    LOG.info("Index Recovery: Stats: {}", statistics);
                }
            } else {
                LOG.error("Transaction stats: Invalid transaction recovery object!: Unexpected type: {}: Details: {}", txRecoveryObject.getClass().toString(), txRecoveryObject);
            }
        } catch (Exception e) {
            LOG.error("Error: Retrieving log transaction stats!", e);
        }
    }

    private static int enableIndex(AtlasJanusGraph graph, JanusGraphIndex index, boolean isTypeSync) {
        PropertyKey[] propertyKeys = index.getFieldKeys();
        SchemaStatus status = index.getIndexStatus(propertyKeys[0]);
        String indexName = index.name();
        int count = 0;

        try {
            if (status == REGISTERED || (isTypeSync && status == INSTALLED)) {

                if (status == INSTALLED) {
                    count = updateIndexStatus(graph, indexName, REGISTER_INDEX, status, REGISTERED);
                    if (count == -1) {
                        LOG.warn("Skipping update schema to {}", ENABLED);
                        return count;
                    }
                }

                status = index.getIndexStatus(propertyKeys[0]);
                count = updateIndexStatus(graph, indexName, ENABLE_INDEX, status, ENABLED);

            } else if (status == INSTALLED) {
                LOG.warn("SchemaStatus {} found for index: {}", INSTALLED, indexName);
            }
        } catch (InterruptedException e) {
            LOG.error("IllegalStateException for indexName : {}, Exception: ", indexName, e);
        } catch (ExecutionException e) {
            LOG.error("ExecutionException for indexName : {}, Exception: ", indexName, e);
        }

        return count;
    }

    private static int updateIndexStatus(AtlasJanusGraph atlasGraph, String indexName, SchemaAction toAction,
                                         SchemaStatus fromStatus, SchemaStatus toStatus) throws ExecutionException, InterruptedException {
        int count = 0;

        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();

        graph.getOpenTransactions().forEach(tx -> graph.closeTransaction((StandardJanusGraphTx) tx));

        JanusGraphManagement management = graph.openManagement();
        JanusGraphIndex indexToUpdate = management.getGraphIndex(indexName);
        LOG.info("SchemaStatus updating for index: {}, from {} to {}.", indexName, fromStatus, toStatus);

        management.updateIndex(indexToUpdate, toAction).get();
        try {
            management.commit();
        } catch (Exception e) {
            LOG.info("Exception while committing, class name: {}", e.getClass().getSimpleName());
            if (e.getClass().getSimpleName().equals("PermanentLockingException")) {
                LOG.info("Commit error! will pause and retry");
                Thread.sleep(5000);
                management.commit();
            }
        }

        GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, indexName).status(toStatus).call();
        LOG.info("SchemaStatus update report: {}", report);

        if (!report.getSucceeded()) {
            LOG.error("SchemaStatus failed to update report: {}", report);
            return -1;
        }

        if (!report.getConvergedKeys().isEmpty() && report.getConvergedKeys().containsKey(indexName)) {
            LOG.info("SchemaStatus updated for index: {}, from {} to {}.", indexName, fromStatus, toStatus);

            count++;
        } else if (!report.getNotConvergedKeys().isEmpty() && report.getNotConvergedKeys().containsKey(indexName)) {
            LOG.error("SchemaStatus failed to update index: {}, from {} to {}.", indexName, fromStatus, toStatus);
        }

        return count;
    }

    private void reindexElement(ManagementSystem managementSystem, IndexSerializer indexSerializer, MixedIndexType indexType, List<AtlasElement> elements) throws Exception {
        Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
        StandardJanusGraphTx tx = managementSystem.getWrappedTx();
        BackendTransaction txHandle = tx.getTxHandle();

        try {
            JanusGraphElement janusGraphElement = null;
            for (AtlasElement element : elements) {
                try {
                    if (element == null || element.getWrappedElement() == null) {
                        continue;
                    }

                    janusGraphElement = element.getWrappedElement();
                    indexSerializer.reindexElement(janusGraphElement, indexType, documentsPerStore);
                } catch (Exception e) {
                    LOG.warn("{}: Exception: {}:{}", indexType.getName(), e.getClass().getSimpleName(), e.getMessage());
                }
            }
        } finally {
            if (txHandle != null) {
                txHandle.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
            }
        }
    }
}
