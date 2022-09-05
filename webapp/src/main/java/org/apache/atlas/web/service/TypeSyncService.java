package org.apache.atlas.web.service;

import org.apache.atlas.annotation.GraphTransaction;
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
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.atlas.service.ActiveIndexNameManager.*;
import static org.janusgraph.graphdb.database.management.ManagementSystem.awaitGraphIndexStatus;

@Component
public class TypeSyncService {

    private static final Logger LOG = LoggerFactory.getLogger(TypeSyncService.class);

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasJanusGraph atlasGraph;
    private final AtlasMixedBackendIndexManager atlasMixedBackendIndexManager;
    private final DefaultIndexCreator defaultIndexCreator;
    private final ElasticInstanceConfigService elasticInstanceConfigService;
    private final AtlasTypeDefStore atlasTypeDefStore;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore,
                           AtlasJanusGraph atlasGraph,
                           AtlasMixedBackendIndexManager atlasMixedBackendIndexManager,
                           DefaultIndexCreator defaultIndexCreator,
                           ElasticInstanceConfigService elasticInstanceConfigService, AtlasTypeDefStore atlasTypeDefStore) {
        this.typeDefStore = typeDefStore;
        this.atlasGraph = atlasGraph;
        this.atlasMixedBackendIndexManager = atlasMixedBackendIndexManager;
        this.defaultIndexCreator = defaultIndexCreator;
        this.elasticInstanceConfigService = elasticInstanceConfigService;
        this.atlasTypeDefStore = atlasTypeDefStore;
    }

    @GraphTransaction
    public TypeSyncResponse syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException, IndexException, RepositoryException, IOException, ExecutionException, InterruptedException {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean haveIndexSettingsChanged = existingTypeDefinitions.haveIndexSettingsChanged(newTypeDefinitions);
        String newIndexName = null;
        if (haveIndexSettingsChanged) {
            newIndexName = elasticInstanceConfigService.updateCurrentIndexName();
            atlasMixedBackendIndexManager.createIndexIfNotExists(newIndexName);
            setCurrentWriteVertexIndexName(newIndexName);
            defaultIndexCreator.createDefaultIndexes(atlasGraph);
        }
        atlasTypeDefStore.updateTypesDef(newTypeDefinitions.getUpdatedTypesDef(existingTypeDefinitions));
        atlasTypeDefStore.createTypesDef(newTypeDefinitions.getCreatedOrDeletedTypesDef(existingTypeDefinitions));
//        atlasTypeDefStore.deleteTypesDef(existingTypeDefinitions.getCreatedOrDeletedTypesDef(newTypeDefinitions));

        return new TypeSyncResponse(
                haveIndexSettingsChanged,
                haveIndexSettingsChanged,
                getCurrentReadVertexIndexName(),
                newIndexName
        );
    }

    public void cleanupTypeSync() {
        String oldIndexName = getCurrentReadVertexIndexName();
        String newIndexName = getCurrentWriteVertexIndexName();
        if (!oldIndexName.equals(newIndexName)) {
            try {
                disableJanusgraphIndex(oldIndexName);
                deleteJanusgraphIndex(oldIndexName);

                atlasMixedBackendIndexManager.deleteIndex(oldIndexName);
            } catch (InterruptedException | ExecutionException | IOException e) {
                LOG.error("Error while deleting index {}. Exception: {}", oldIndexName, e.toString());
            }

            setCurrentReadVertexIndexName(newIndexName);
        }
    }

    private void disableJanusgraphIndex(String oldIndexName) throws InterruptedException, ExecutionException {
        JanusGraphManagement janusGraphManagement = atlasGraph.getGraph().openManagement();

        JanusGraphIndex graphIndex = janusGraphManagement.getGraphIndex(oldIndexName);
        janusGraphManagement.updateIndex(graphIndex, SchemaAction.DISABLE_INDEX).get();
        janusGraphManagement.commit();
        atlasGraph.getGraph().tx().commit();
        GraphIndexStatusReport report = awaitGraphIndexStatus(atlasGraph.getGraph(), oldIndexName).status(SchemaStatus.DISABLED).call();
        System.out.println(report);
    }

    private void deleteJanusgraphIndex(String oldIndexName) throws InterruptedException, ExecutionException {
        JanusGraphManagement janusGraphManagement = atlasGraph.getGraph().openManagement();
        JanusGraphIndex graphIndex = janusGraphManagement.getGraphIndex(oldIndexName);
        JanusGraphManagement.IndexJobFuture indexJobFuture = janusGraphManagement.updateIndex(graphIndex, SchemaAction.REMOVE_INDEX);
        janusGraphManagement.commit();
        atlasGraph.getGraph().tx().commit();
        indexJobFuture.get();
    }
}
