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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.indexmanager.*;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.Constants.VERTEX_INDEX;


/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
@Component
@Order(1)
public class GraphBackedSearchIndexer extends GraphTransactionManager implements SearchIndexer, ActiveStateChangeHandler, TypeDefChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    // Added for type lookup when indexing the new typedefs
    private final AtlasTypeRegistry typeRegistry;
    private final List<IndexChangeListener> indexChangeListeners = new ArrayList<>();

    private final GraphBackedIndexCreator graphBackedIndexCreator;
    private final TypedefIndexCreator typedefIndexCreator;
    private final IndexFieldNameResolver indexFieldNameResolver;
    private final VertexIndexCreator vertexIndexCreator;
    //allows injection of a dummy graph for testing
    private IAtlasGraphProvider provider;

    private Set<String> vertexIndexKeys = new HashSet<>();

    public static boolean isValidSearchWeight(int searchWeight) {
        if (searchWeight != -1) {
            return searchWeight >= 1 && searchWeight <= 10;
        }
        return true;
    }

    public static boolean isStringAttribute(AtlasAttribute attribute) {
        return AtlasBaseTypeDef.ATLAS_TYPE_STRING.equals(attribute.getTypeName());
    }

    public enum UniqueKind {NONE, GLOBAL_UNIQUE, PER_TYPE_UNIQUE}

    @Inject
    public GraphBackedSearchIndexer(AtlasTypeRegistry typeRegistry, GraphBackedIndexCreator graphBackedIndexCreator, TypedefIndexCreator typedefIndexCreator, IndexFieldNameResolver indexFieldNameResolver, VertexIndexCreator vertexIndexCreator) throws AtlasException {
        this(new AtlasGraphProvider(), ApplicationProperties.get(), typeRegistry, graphBackedIndexCreator, typedefIndexCreator, indexFieldNameResolver, vertexIndexCreator);
    }

    @VisibleForTesting
    GraphBackedSearchIndexer(IAtlasGraphProvider provider, Configuration configuration, AtlasTypeRegistry typeRegistry, GraphBackedIndexCreator graphBackedIndexCreator, TypedefIndexCreator typedefIndexCreator, IndexFieldNameResolver indexFieldNameResolver, VertexIndexCreator vertexIndexCreator)
            throws IndexException, RepositoryException {
        this.provider = provider;
        this.typeRegistry = typeRegistry;
        this.graphBackedIndexCreator = graphBackedIndexCreator;
        this.typedefIndexCreator = typedefIndexCreator;
        this.indexFieldNameResolver = indexFieldNameResolver;
        this.vertexIndexCreator = vertexIndexCreator;

        //make sure solr index follows graph backed index listener
        addIndexListener(new SolrIndexHelper(typeRegistry));

        if (!HAConfiguration.isHAEnabled(configuration)) {
            graphBackedIndexCreator.createDefaultIndexes(provider.get());
        }
        notifyInitializationStart();
    }

    public void addIndexListener(IndexChangeListener listener) {
        indexChangeListeners.add(listener);
    }

    /**
     * Initialize global indices for JanusGraph on server activation.
     *
     * Since the indices are shared state, we need to do this only from an active instance.
     */
    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("Reacting to active: initializing index");
        try {
            graphBackedIndexCreator.createDefaultIndexes(provider.get());
        } catch (RepositoryException | IndexException e) {
            throw new AtlasException("Error in reacting to active on initialization", e);
        }
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive state: No action right now.");
    }

    @Override
    public int getHandlerOrder() {
        return HandlerOrder.GRAPH_BACKED_SEARCH_INDEXER.getOrder();
    }

    @Override
    public void onChange(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        typedefIndexCreator.createIndexForTypedefs(changedTypeDefs);
    }

    @Override
    public void onLoadCompletion() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Type definition load completed. Informing the completion to IndexChangeListeners.");
        }

        Collection<AtlasBaseTypeDef> typeDefs = new ArrayList<>();

        typeDefs.addAll(typeRegistry.getAllEntityDefs());
        typeDefs.addAll(typeRegistry.getAllBusinessMetadataDefs());

        ChangedTypeDefs changedTypeDefs = new ChangedTypeDefs(null, new ArrayList<>(typeDefs), null);
        AtlasGraphManagement management = null;

        try {
            management = provider.get().getManagementSystem();

            //resolve index fields names
            indexFieldNameResolver.resolveIndexFieldNames(management, changedTypeDefs);

            //Commit indexes
            commit(management);

            notifyInitializationCompletion(changedTypeDefs);
        } catch (RepositoryException | IndexException e) {
            LOG.error("Failed to update indexes for changed typedefs", e);
            attemptRollback(changedTypeDefs, management);
        }
    }

    public Set<String> getVertexIndexKeys() {
        if (recomputeIndexedKeys) {
            AtlasGraphManagement management = null;

            try {
                management = provider.get().getManagementSystem();

                if (management != null) {
                    AtlasGraphIndex vertexIndex = management.getGraphIndex(VERTEX_INDEX);

                    if (vertexIndex != null) {
                        recomputeIndexedKeys = false;

                        Set<String> indexKeys = new HashSet<>();

                        for (AtlasPropertyKey fieldKey : vertexIndex.getFieldKeys()) {
                            indexKeys.add(fieldKey.getName());
                        }

                        vertexIndexKeys = indexKeys;
                    }

                    management.commit();
                }
            } catch (Exception excp) {
                LOG.error("getVertexIndexKeys(): failed to get indexedKeys from graph", excp);

                if (management != null) {
                    try {
                        management.rollback();
                    } catch (Exception e) {
                        LOG.error("getVertexIndexKeys(): rollback failed", e);
                    }
                }
            }
        }

        return vertexIndexKeys;
    }

    public String createVertexIndex(AtlasGraphManagement management, String propertyName, UniqueKind uniqueKind, Class propertyClass,
                                    AtlasCardinality cardinality, boolean createCompositeIndex, boolean createCompositeIndexWithTypeAndSuperTypes, boolean isStringField, HashMap<String, Object> indexTypeESConfig, HashMap<String, HashMap<String, Object>> indexTypeESFields) {
        return vertexIndexCreator.createVertexIndex(
                management,
                propertyName,
                uniqueKind,
                propertyClass,
                cardinality,
                createCompositeIndex,
                createCompositeIndexWithTypeAndSuperTypes,
                isStringField,
                indexTypeESConfig,
                indexTypeESFields
        );
    }

    private void notifyInitializationStart() {
        for (IndexChangeListener indexChangeListener : indexChangeListeners) {
            try {
                indexChangeListener.onInitStart();
            } catch (Throwable t) {
                LOG.error("Error encountered in notifying the index change listener {}.", indexChangeListener.getClass().getName(), t);
                //we need to throw exception if any of the listeners throw execption.
                throw new RuntimeException("Error encountered in notifying the index change listener " + indexChangeListener.getClass().getName(), t);
            }
        }
    }

    private void notifyInitializationCompletion(ChangedTypeDefs changedTypeDefs) {
        for (IndexChangeListener indexChangeListener : indexChangeListeners) {
            try {
                indexChangeListener.onInitCompletion(changedTypeDefs);
            } catch (Throwable t) {
                LOG.error("Error encountered in notifying the index change listener {}.", indexChangeListener.getClass().getName(), t);
                //we need to throw exception if any of the listeners throw execption.
                throw new RuntimeException("Error encountered in notifying the index change listener " + indexChangeListener.getClass().getName(), t);
            }
        }
    }


}
