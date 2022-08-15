package org.apache.atlas.web.service;

import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.indexmanager.DefaultIndexCreator;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexCreator;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;

@Component
public class TypeSyncService {

    private final AtlasTypeDefStore typeDefStore;
    private final DefaultIndexCreator defaultIndexCreator;
    private final AtlasGraph atlasGraph;
    private final AtlasIndexCreator atlasIndexCreator;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore, DefaultIndexCreator defaultIndexCreator, AtlasGraph atlasGraph, AtlasIndexCreator atlasIndexCreator) {
        this.typeDefStore = typeDefStore;
        this.defaultIndexCreator = defaultIndexCreator;
        this.atlasGraph = atlasGraph;
        this.atlasIndexCreator = atlasIndexCreator;
    }

    @GraphTransaction
    public AtlasTypesDef syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException, IndexException, RepositoryException, IOException {
//        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
//        boolean hasIndexSettingsChanged = existingTypeDefinitions.hasIndexSettingsChanged(newTypeDefinitions);
        atlasIndexCreator.createIndexIfNotExists("vertex_index_845");
        ActiveIndexNameManager.setCurrentIndexName("vertex_index_845");

        defaultIndexCreator.createDefaultIndexes(atlasGraph);
        return null;
    }

}
