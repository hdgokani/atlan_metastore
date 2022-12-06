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
    public TypeSyncResponse syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException, IndexException, RepositoryException, IOException {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean haveIndexSettingsChanged = existingTypeDefinitions.haveIndexSettingsChanged(newTypeDefinitions);
        String newIndexName = null;
        try {
            if (haveIndexSettingsChanged) {
                newIndexName = elasticInstanceConfigService.updateCurrentIndexName();
                LOG.info("newIndexName: {}", newIndexName);

                atlasMixedBackendIndexManager.createIndexIfNotExists(newIndexName);
                setCurrentWriteVertexIndexName(newIndexName);

                StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();
                graph.getOpenTransactions().forEach(tx -> graph.closeTransaction((StandardJanusGraphTx) tx));

                defaultIndexCreator.createDefaultIndexes(atlasGraph);
            }
            AtlasTypesDef toUpdate = newTypeDefinitions.getUpdatedTypesDef(existingTypeDefinitions);
            AtlasTypesDef toCreate = newTypeDefinitions.getCreatedOrDeletedTypesDef(existingTypeDefinitions);

            LOG.info("toUpdate entity {}", toUpdate.getEntityDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toUpdate enum {}", toUpdate.getEnumDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toUpdate class {}", toUpdate.getClassificationDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toUpdate BM {}", toUpdate.getBusinessMetadataDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toUpdate relation {}", toUpdate.getRelationshipDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));

            LOG.info("toCreate entity {}", toCreate.getEntityDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toCreate enum {}", toCreate.getEnumDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toCreate class {}", toCreate.getClassificationDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toCreate BM {}", toCreate.getBusinessMetadataDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));
            LOG.info("toCreate relation {}", toCreate.getRelationshipDefs().stream().map(x -> x.getName()).collect(Collectors.joining(", ")));

            typeDefStore.createTypesDef(toCreate);
            typeDefStore.updateTypesDef(toUpdate);

            //atlasGraph.getManagementSystem().enableIndexForTypeSync();
        } catch (Exception e){
            setCurrentWriteVertexIndexName(getCurrentReadVertexIndexName());
            //TODO: rollback instance config entity
            throw e;
        }

        return new TypeSyncResponse(
                haveIndexSettingsChanged,
                haveIndexSettingsChanged,
                getCurrentReadVertexIndexName(),
                newIndexName
        );
    }

    public void cleanupTypeSync(String traceId) {
        String oldIndexName = getCurrentReadVertexIndexName();
        String newIndexName = getCurrentWriteVertexIndexName();

        if (!oldIndexName.equals(newIndexName)) {
            setCurrentReadVertexIndexName(newIndexName);
            LOG.info("Redirected ES reads to new index {}", newIndexName);

            try {
                disableJanusgraphIndex(oldIndexName);
                atlasMixedBackendIndexManager.deleteIndex(oldIndexName);

                LOG.info("Deleted old index {}", oldIndexName);
            } catch (InterruptedException | ExecutionException | IOException e) {
                LOG.error("Error while deleting index {}. Exception: {}", oldIndexName, e.toString());
            }
        }
    }

    public void testCreateIndex(String ndexName) throws InterruptedException, ExecutionException {
        try {
            atlasMixedBackendIndexManager.createIndexIfNotExists(ndexName);
            setCurrentWriteVertexIndexName(ndexName);

            defaultIndexCreator.createDefaultIndexes(atlasGraph);
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disableJanusgraphIndex(String oldIndexName) throws InterruptedException, ExecutionException {
        updateIndexStatus(atlasGraph, oldIndexName, DISABLE_INDEX, DISABLED);
    }

    private int updateIndexStatus(AtlasJanusGraph atlasGraph, String indexName,
                                  SchemaAction toAction, SchemaStatus toStatus) throws ExecutionException, InterruptedException {
        int count = 0;

        StandardJanusGraph graph = (StandardJanusGraph) atlasGraph.getGraph();

        closeOpenTransactions(graph);

        JanusGraphManagement management = null;
        try {
            closeOpenInstances(graph);

            LOG.info("Open transactions after opening new management {}", graph.getOpenTransactions().size());

            management = graph.openManagement();
            JanusGraphIndex indexToUpdate = management.getGraphIndex(indexName);
            SchemaStatus fromStatus = indexToUpdate.getIndexStatus(indexToUpdate.getFieldKeys()[0]);

            LOG.info("SchemaStatus updating for index: {}, from {} to {}.", indexName, fromStatus, toStatus);

            management.updateIndex(indexToUpdate, toAction).get();
            try {
                management.commit();
                graph.tx().commit();
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
            if (management != null) {
                management.rollback();
            }
        }

        return count;
    }

    private void closeOpenTransactions (StandardJanusGraph graph) {
        LOG.info("Open transactions {}", graph.getOpenTransactions().size());

        //graph.getOpenTransactions().forEach(tx -> graph.closeTransaction((StandardJanusGraphTx) tx));
        //graph.getOpenTransactions().forEach(JanusGraphTransaction::commit);
        graph.getOpenTransactions().forEach(JanusGraphTransaction::rollback);

        LOG.info("Open transactions after closing {}", graph.getOpenTransactions().size());
    }

    private void closeOpenInstances(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();

        try {
            LOG.info("Open instances {}", management.getOpenInstances().size());
            LOG.info("Open instances");

            Set<String> openInstances = management.getOpenInstances();
            openInstances.forEach(LOG::info);

            if (CollectionUtils.isNotEmpty(openInstances)) {
                openInstances.forEach(LOG::info);

                openInstances.stream().filter(x -> !x.contains("current")).forEach(management::forceCloseInstance);
            }
            LOG.info("Closed all other instances");
        } finally {
            management.rollback();
        }
    }
}
