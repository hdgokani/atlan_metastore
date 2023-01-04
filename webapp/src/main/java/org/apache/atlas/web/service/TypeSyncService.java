package org.apache.atlas.web.service;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.TypeCacheRefresher;
import org.apache.atlas.repository.graph.indexmanager.DefaultIndexCreator;
import org.apache.atlas.repository.graphdb.AtlasMixedBackendIndexManager;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.web.dto.TypeSyncResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
import java.util.List;
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
    private final EntityGraphRetriever graphRetriever;
    private final AtlasMixedBackendIndexManager atlasMixedBackendIndexManager;
    private final DefaultIndexCreator defaultIndexCreator;
    private final ElasticInstanceConfigService elasticInstanceConfigService;
    private final TaskManagement taskManagement;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore,
                           AtlasJanusGraph atlasGraph,
                           EntityGraphRetriever graphRetriever,
                           AtlasMixedBackendIndexManager atlasMixedBackendIndexManager,
                           DefaultIndexCreator defaultIndexCreator,
                           ElasticInstanceConfigService elasticInstanceConfigService,
                           TaskManagement taskManagement) {
        this.typeDefStore = typeDefStore;
        this.atlasGraph = atlasGraph;
        this.graphRetriever = graphRetriever;
        this.atlasMixedBackendIndexManager = atlasMixedBackendIndexManager;
        this.defaultIndexCreator = defaultIndexCreator;
        this.elasticInstanceConfigService = elasticInstanceConfigService;
        this.taskManagement = taskManagement;
    }

    public void waitAllRequestsToComplete(String traceId) {

        LOG.info("Waiting for all active requests until done");
        RequestContext.setIsTypeSyncMode(true);

        while (true) {
            Set<RequestContext> activeRequests = RequestContext.getActiveRequests();

            //kill in progress task
            if (taskManagement != null) {
                try {
                    taskManagement.terminateInProgressTasks();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (activeRequests.size() == 2) {
                int vTh = 0;

                for (RequestContext req : activeRequests) {
                    if (req.getThId() == 1 || req.getThId() == RequestContext.get().getThId()) {
                        vTh++;
                    }
                }

                if (vTh ==2) {
                    LOG.info("No other active request found!!");
                    break;
                }
            }

            StringBuilder sb = new StringBuilder();

            for (RequestContext acReq : activeRequests) {
                sb.append("thId: ").append(acReq.getThId()).append(", thName: ").append(acReq.getThName()).append(", ");
            }

            LOG.info("activeRequests({}): {}", activeRequests.size(), sb.toString());
            LOG.info("current thread id: {}", RequestContext.get().getThId());

            try {
                LOG.info("Sleeping for 15 seconds to check for Active requests");
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //@GraphTransaction
    public TypeSyncResponse syncTypes(AtlasTypesDef newTypeDefinitions, final TypeCacheRefresher typeCacheRefresher) throws Exception {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean haveIndexSettingsChanged = existingTypeDefinitions.haveIndexSettingsChanged(newTypeDefinitions);
        String newIndexName = null;
        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();
        LOG.info("### 1");

        try {
            if (haveIndexSettingsChanged) {

                RequestContext.setIsTypeSyncMode(true);
                typeCacheRefresher.refreshAllHostCache(TypeCacheRefresher.RefreshOperation.WAIT_COMPLETE_REQUESTS.getId());
                waitAllRequestsToComplete(RequestContext.get().getTraceId());

                LOG.info("### 2");
                newIndexName = elasticInstanceConfigService.updateCurrentIndexName();LOG.info("### 3");
                LOG.info("newIndexName: {}", newIndexName);

                atlasMixedBackendIndexManager.createIndexIfNotExists(newIndexName);LOG.info("### 4");

                setCurrentWriteVertexIndexName(newIndexName);

                closeOpenTransactions(graph);LOG.info("### 5");

                graph.tx().rollback();LOG.info("### 7");

                defaultIndexCreator.createDefaultIndexes(atlasGraph);LOG.info("### 8");

                /*LOG.info("Waiting for 20 seconds");
                Thread.sleep(20000);
                LOG.info("Wait over");*/

                GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, newIndexName).call();
                LOG.info("report after creating new index {}", report.toString());LOG.info("### 9");
            }

            AtlasTypesDef toUpdate = newTypeDefinitions.getUpdatedTypesDef(existingTypeDefinitions);LOG.info("### 10");
            AtlasTypesDef toCreate = newTypeDefinitions.getCreatedOrDeletedTypesDef(existingTypeDefinitions);LOG.info("### 11");

            typeDefStore.createTypesDef(toCreate);LOG.info("### 12");

            typeDefStore.updateTypesDef(toUpdate);LOG.info("### 13");

            if (haveIndexSettingsChanged) {
                /*LOG.info("Waiting for 120 seconds");
                Thread.sleep(120000);
                LOG.info("Wait over");*/

                GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, newIndexName).call();
                LOG.info("report after update typesDef new index {}", report.toString());
                LOG.info("### 14");
            }

        } catch (Exception e) {
            LOG.error("Failed to sync typesDef: rollback needed:" + haveIndexSettingsChanged, e);

            if (haveIndexSettingsChanged) {
                setCurrentWriteVertexIndexName(getCurrentReadVertexIndexName());

                RequestContext.setIsTypeSyncMode(false);
                typeCacheRefresher.refreshAllHostCache(TypeCacheRefresher.RefreshOperation.READ_INDEX.getId());

                try {
                    elasticInstanceConfigService.rollbackCurrentIndexName();
                } catch (Exception e0) {
                    LOG.error("Failed to rollback elastic Instance Config entity", e0);
                }

                if (StringUtils.isNotEmpty(newIndexName)) {
                    try {
                        atlasMixedBackendIndexManager.deleteIndex(newIndexName);
                    } catch (Exception e0) {
                        LOG.error("Failed to delete elastic index", e0);
                    }

                    disableJanusgraphIndex(newIndexName);
                }
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

    public boolean cleanupTypeSync(String traceId, TypeCacheRefresher typeCacheRefresher) throws AtlasBaseException, InterruptedException {
        String oldIndexName = getCurrentReadVertexIndexName();
        String newIndexName = getCurrentWriteVertexIndexName();
        LOG.info("cleanupTypeSync: oldIndexName:{}, newIndexName:{}", oldIndexName, newIndexName);

        if (!oldIndexName.equals(newIndexName)) {
            setCurrentReadVertexIndexName(newIndexName);
            LOG.info("Redirected ES reads to new index {}", newIndexName);

            try {
                disableJanusgraphIndex(oldIndexName);
                atlasMixedBackendIndexManager.deleteIndex(oldIndexName);
                
                return true;
            } catch (Exception e) {
                LOG.error("Error while disabling/deleting index {}. Exception: {}, Rolling back...", oldIndexName, e);

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
            } finally {
                RequestContext.setIsTypeSyncMode(false);
            }
        }
        return false;
    }

    public void disableJanusgraphIndex(String oldIndexName) throws InterruptedException, AtlasBaseException {
        updateIndexStatus(atlasGraph, oldIndexName, DISABLE_INDEX, DISABLED);
    }

    private void updateIndexStatus(AtlasJanusGraph atlasGraph, String indexName,
                                  SchemaAction toAction, SchemaStatus toStatus) throws InterruptedException, AtlasBaseException {
        int retry = 3;

        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();

        for (int attempt = 1; attempt <= retry; attempt++) {
            LOG.info("Disable index attempt {}", attempt);

            closeOpenTransactions(graph);

            JanusGraphManagement management = null;
            try {
                closeOpenInstances(graph);

                management = graph.openManagement();

                JanusGraphIndex indexToUpdate = management.getGraphIndex(indexName);
                SchemaStatus fromStatus = indexToUpdate.getIndexStatus(indexToUpdate.getFieldKeys()[0]);

                if (fromStatus == toStatus) {
                    LOG.warn("Skipping Index status update as index already in {}", fromStatus);
                    return;
                }

                LOG.info("SchemaStatus updating for index: {}, from {} to {}.", indexName, fromStatus, toStatus);

                management.updateIndex(indexToUpdate, toAction).get();
                try {
                    management.commit();
                } catch (Exception e) {
                    LOG.error("Exception while committing:", e);
                    throw new AtlasBaseException(e);
                }

                /*LOG.info("Waiting for 60 seconds");
                Thread.sleep(60000);
                LOG.info("Wait over");*/

                LOG.info("Waiting for 120 seconds to update status");
                GraphIndexStatusReport report = ManagementSystem
                        .awaitGraphIndexStatus(graph, indexName)
                        .status(toStatus)
                        .timeout(120, ChronoUnit.SECONDS)
                        .call();
                LOG.info("SchemaStatus update report: {}", report);

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
                LOG.error("All attempts exhausted, failed to Disable index {}", indexName);
                throw new AtlasBaseException("All attempts exhausted, failed to Disable index " + indexName);

            } else {
                LOG.info("Sleeping for 60 seconds before re-attempting");
                Thread.sleep(60000);
            }
        }
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

    private void closeOpenInstances(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();

        try {
            Set<String> openInstances = management.getOpenInstances();

            LOG.info("Open instances {}", openInstances.size());
            LOG.info("Open instances");

            if (CollectionUtils.isNotEmpty(openInstances)) {
                openInstances.forEach(LOG::info);

                openInstances.stream().filter(x -> !x.contains("current")).forEach(management::forceCloseInstance);
            }
            LOG.info("Closed all other instances");
        } catch (Exception e) {
            LOG.error("Failed to close open instances", e);
        } finally {
            management.commit();
        }
    }
}
