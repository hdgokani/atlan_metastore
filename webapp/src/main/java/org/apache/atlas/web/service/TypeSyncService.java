package org.apache.atlas.web.service;

import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.indexmanager.DefaultIndexCreator;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexCreator;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;

@Order
@Component
public class TypeSyncService {

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasGraph atlasGraph;
    private final AtlasIndexCreator atlasIndexCreator;
    private final DefaultIndexCreator defaultIndexCreator;
    private final ElasticInstanceConfigService elasticInstanceConfigService;
    private final AtlasTypeDefStore atlasTypeDefStore;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore,
                           AtlasGraph atlasGraph,
                           AtlasIndexCreator atlasIndexCreator,
                           DefaultIndexCreator defaultIndexCreator,
                           ElasticInstanceConfigService elasticInstanceConfigService, AtlasTypeDefStore atlasTypeDefStore) {
        this.typeDefStore = typeDefStore;
        this.atlasGraph = atlasGraph;
        this.atlasIndexCreator = atlasIndexCreator;
        this.defaultIndexCreator = defaultIndexCreator;
        this.elasticInstanceConfigService = elasticInstanceConfigService;
        this.atlasTypeDefStore = atlasTypeDefStore;
    }


    @GraphTransaction
    public AtlasTypesDef syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException, IndexException, RepositoryException, IOException {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean haveIndexSettingsChanged = existingTypeDefinitions.hasIndexSettingsChanged(newTypeDefinitions);
        if (haveIndexSettingsChanged) {
            String newIndexName = elasticInstanceConfigService.updateCurrentIndexName();
            atlasIndexCreator.createIndexIfNotExists(newIndexName);
            ActiveIndexNameManager.setCurrentWriteVertexIndexName(newIndexName);
            defaultIndexCreator.createDefaultIndexes(atlasGraph);
            ActiveIndexNameManager.setCurrentReadVertexIndexName(newIndexName);
        }
        return atlasTypeDefStore.updateTypesDef(newTypeDefinitions);
    }

}
