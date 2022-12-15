package org.apache.atlas.web.service;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.indexmanager.DefaultIndexCreator;
import org.apache.atlas.repository.graphdb.AtlasMixedBackendIndexManager;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.dto.TypeSyncResponse;
import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.atlas.service.ActiveIndexNameManager.*;
import static org.janusgraph.core.schema.SchemaAction.DISABLE_INDEX;
import static org.janusgraph.core.schema.SchemaStatus.DISABLED;
import static org.janusgraph.core.schema.SchemaStatus.ENABLED;
import static org.janusgraph.graphdb.database.management.ManagementSystem.awaitGraphIndexStatus;

@Component
public class TypeSyncService {

    private static final Logger LOG = LoggerFactory.getLogger(TypeSyncService.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasJanusGraph atlasGraph;
    private final AtlasMixedBackendIndexManager atlasMixedBackendIndexManager;
    private final DefaultIndexCreator defaultIndexCreator;
    private final ElasticInstanceConfigService elasticInstanceConfigService;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore,
                           AtlasJanusGraph atlasGraph,
                           AtlasMixedBackendIndexManager atlasMixedBackendIndexManager,
                           DefaultIndexCreator defaultIndexCreator,
                           ElasticInstanceConfigService elasticInstanceConfigService) {
        this.typeDefStore = typeDefStore;
        this.atlasGraph = atlasGraph;
        this.atlasMixedBackendIndexManager = atlasMixedBackendIndexManager;
        this.defaultIndexCreator = defaultIndexCreator;
        this.elasticInstanceConfigService = elasticInstanceConfigService;
    }

    //@GraphTransaction
    public TypeSyncResponse syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException, IndexException, RepositoryException, IOException, InterruptedException {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean haveIndexSettingsChanged = existingTypeDefinitions.haveIndexSettingsChanged(newTypeDefinitions);
        String newIndexName = null;
        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();
        LOG.info("### 1");

        try {
            if (haveIndexSettingsChanged) {
                LOG.info("### 2");
                newIndexName = elasticInstanceConfigService.updateCurrentIndexName();LOG.info("### 3");
                LOG.info("newIndexName: {}", newIndexName);

                atlasMixedBackendIndexManager.createIndexIfNotExists(newIndexName);LOG.info("### 4");

                setCurrentWriteVertexIndexName(newIndexName);

                closeOpenTransactions(graph);LOG.info("### 5");

                graph.tx().rollback();LOG.info("### 7");

                defaultIndexCreator.createDefaultIndexes(atlasGraph);LOG.info("### 8");

                LOG.info("Waiting for 20 seconds");
                Thread.sleep(20000);
                LOG.info("Wait over");
            }

            if (haveIndexSettingsChanged) {
                GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, newIndexName).call();
                LOG.info("report after creating new index {}", report.toString());LOG.info("### 9");
            }

            AtlasTypesDef toUpdate = newTypeDefinitions.getUpdatedTypesDef(existingTypeDefinitions);LOG.info("### 10");
            AtlasTypesDef toCreate = newTypeDefinitions.getCreatedOrDeletedTypesDef(existingTypeDefinitions);LOG.info("### 11");

            typeDefStore.createTypesDef(toCreate);LOG.info("### 12");

            typeDefStore.updateTypesDef(toUpdate);LOG.info("### 13");

            if (haveIndexSettingsChanged) {
                LOG.info("Waiting for 120 seconds");
                Thread.sleep(120000);
                LOG.info("Wait over");

                GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, newIndexName).call();
                LOG.info("report after update typesDef new index {}", report.toString());
                LOG.info("### 14");
            }

        } catch (Exception e) {
            LOG.error("Failed to sync typesDef: rollback needed:" + haveIndexSettingsChanged, e);

            if (haveIndexSettingsChanged) {
                setCurrentWriteVertexIndexName(getCurrentReadVertexIndexName());

                try {
                    elasticInstanceConfigService.rollbackCurrentIndexName();
                } catch (Exception e0) {
                    LOG.error("Failed to rollback elastic Instance Config entity", e0);
                }

                try {
                    atlasMixedBackendIndexManager.deleteIndex(newIndexName);
                } catch (Exception e0) {
                    LOG.error("Failed to delete elastic index", e0);
                }

                disableJanusgraphIndex(newIndexName);
            }

            throw e;
        }

        return new TypeSyncResponse(
                haveIndexSettingsChanged,
                haveIndexSettingsChanged,
                getCurrentReadVertexIndexName(),
                newIndexName
        );
    }

    public boolean cleanupTypeSync(String traceId) throws AtlasBaseException, InterruptedException {
        String oldIndexName = getCurrentReadVertexIndexName();
        String newIndexName = getCurrentWriteVertexIndexName();

        if (!oldIndexName.equals(newIndexName)) {
            setCurrentReadVertexIndexName(newIndexName);
            LOG.info("Redirected ES reads to new index {}", newIndexName);

            try {
                disableJanusgraphIndex(oldIndexName);
                atlasMixedBackendIndexManager.deleteIndex(oldIndexName);

                LOG.info("Deleted old index {}", oldIndexName);
                return true;
            } catch (Exception e) {
                LOG.error("Error while disabling/deleting index {}. Exception: {}", oldIndexName, e);

                setCurrentWriteVertexIndexName(oldIndexName);
                setCurrentReadVertexIndexName(oldIndexName);

                try {
                    elasticInstanceConfigService.rollbackCurrentIndexName();
                } catch (Exception e0) {
                    LOG.error("Failed to rollback elastic Instance Config entity", e0);
                }

                try {
                    atlasMixedBackendIndexManager.deleteIndex(newIndexName);
                } catch (Exception e0) {
                    LOG.error("Failed to delete elastic index", e0);
                }

                disableJanusgraphIndex(newIndexName);
                throw new AtlasBaseException(e);
            }
        }
        return false;
    }

    public void testCreateIndex(String ndexName) throws InterruptedException, ExecutionException {
        try {

            atlasMixedBackendIndexManager.createIndexIfNotExists(ndexName); //ES index creation
            setCurrentWriteVertexIndexName(ndexName); //set static variable

            StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();
            closeOpenTransactions(graph);
            closeOpenInstances(graph);

            graph.tx().rollback();
            defaultIndexCreator.createDefaultIndexes(atlasGraph);
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (AtlasBaseException e) {
            e.printStackTrace();
        }
    }

    public void disableJanusgraphIndex(String oldIndexName) throws InterruptedException, AtlasBaseException {
        updateIndexStatus(atlasGraph, oldIndexName, DISABLE_INDEX, DISABLED);
    }

    private int updateIndexStatus(AtlasJanusGraph atlasGraph, String indexName,
                                  SchemaAction toAction, SchemaStatus toStatus) throws InterruptedException, AtlasBaseException {
        int count = 0;
        int retry = 3;

        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();

        for (int attempt = 1; attempt <= retry; attempt++) {
            LOG.info("Disable index attempt {}", attempt);
            count = 0;

            closeOpenTransactions(graph);

            JanusGraphManagement management = null;
            try {
                closeOpenInstances(graph);

                management = graph.openManagement();
                LOG.info("Open transactions after opening new management {}", graph.getOpenTransactions().size());

                Set<String> openInstances = management.getOpenInstances();
                LOG.info("Open instances after closing all other instance: {}", openInstances.size());
                openInstances.forEach(LOG::info);

                JanusGraphIndex indexToUpdate = management.getGraphIndex(indexName);
                SchemaStatus fromStatus = indexToUpdate.getIndexStatus(indexToUpdate.getFieldKeys()[0]);

                LOG.info("SchemaStatus updating for index: {}, from {} to {}.", indexName, fromStatus, toStatus);

                management.updateIndex(indexToUpdate, toAction).get();
                try {
                    management.commit();
                } catch (Exception e) {
                    LOG.error("Exception while committing:", e);
                    throw new AtlasBaseException(e);
                }

                LOG.info("Waiting for 60 seconds");
                Thread.sleep(60000);
                LOG.info("Wait over");

                LOG.info("Waiting for 120 seconds to update status");
                GraphIndexStatusReport report = ManagementSystem
                        .awaitGraphIndexStatus(graph, indexName)
                        .status(toStatus)
                        .timeout(120, ChronoUnit.SECONDS)
                        .call();
                LOG.info("SchemaStatus update report: {}", report);

                if (!report.getSucceeded()) {
                    LOG.error("SchemaStatus failed to update for index: {}, from {} to {}", indexName, fromStatus, toStatus);
                    return -1;
                }

                if (!report.getConvergedKeys().isEmpty() && report.getConvergedKeys().containsKey(indexName)) {
                    LOG.info("SchemaStatus updated for index: {}, from {} to {}.", indexName, fromStatus, toStatus);
                    count++;

                } else if (!report.getNotConvergedKeys().isEmpty() && report.getNotConvergedKeys().containsKey(indexName)) {
                    LOG.error("SchemaStatus failed to update index: {}, from {} to {}.", indexName, fromStatus, toStatus);
                }
            } catch (Exception e) {
                LOG.error("Failed to updateIndexStatus");

                if (management != null) {
                    management.rollback();
                }

                throw new AtlasBaseException(e);
            }

            if (isDisabled(graph, indexName)) {
                LOG.info("Index disabled successfully");
                break;
            }

            if (attempt == retry) {
                LOG.error("All attempts exhausted, failed to Disable index");
                throw new AtlasBaseException("All attempts exhausted, failed to Disable index");

            } else {
                LOG.info("Sleeping for 60 seconds before re-attempting");
                Thread.sleep(60000);
            }
        }

        return count;
    }

    private boolean isDisabled(StandardJanusGraph graph, String indexName) {
        JanusGraphManagement management = graph.openManagement();

        try {
            JanusGraphIndex indexToUpdate = management.getGraphIndex(indexName);
            SchemaStatus status = indexToUpdate.getIndexStatus(indexToUpdate.getFieldKeys()[0]);

            return status == DISABLED;
        } finally {
            management.commit();
        }
    }

    private void closeOpenTransactions(StandardJanusGraph graph) throws AtlasBaseException {
        LOG.info("Open transactions {}", graph.getOpenTransactions().size());

        try {
            graph.getOpenTransactions().forEach(this::rollbackTxn);
            graph.tx().commit();
        } catch (Exception e) {
            LOG.error("Failed to close open transaction", e);
        }

        LOG.info("Open transactions after closing {}", graph.getOpenTransactions().size());
    }

    private void rollbackTxn(JanusGraphTransaction txn) {
        try {
            txn.rollback();
        } catch (Exception e) {

        }
    }

    private void closeOpenInstances(StandardJanusGraph graph) throws AtlasBaseException {
        JanusGraphManagement management = graph.openManagement();

        try {
            LOG.info("Open instances {}", management.getOpenInstances().size());
            LOG.info("Open instances");

            Set<String> openInstances = management.getOpenInstances();

            if (CollectionUtils.isNotEmpty(openInstances)) {
                openInstances.forEach(LOG::info);

                openInstances.stream().filter(x -> !x.contains("current")).forEach(management::forceCloseInstance);
            }
            LOG.info("Closed all other instances");
        } catch (Exception e) {
            LOG.error("Failed to close open instances", e);
            //throw new AtlasBaseException(e);
        } finally {
            management.commit();
        }
    }
}
