package org.apache.atlas.web.service;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class TypeSyncService {

    private final AtlasTypeDefStore typeDefStore;

    @Inject
    public TypeSyncService(AtlasTypeDefStore typeDefStore) {
        this.typeDefStore = typeDefStore;
    }

    public AtlasTypesDef syncTypes(AtlasTypesDef newTypeDefinitions) throws AtlasBaseException {
        AtlasTypesDef existingTypeDefinitions = typeDefStore.searchTypesDef(new SearchFilter());
        boolean hasIndexSettingsChanged = existingTypeDefinitions.hasIndexSettingsChanged(newTypeDefinitions);


        System.out.println(hasIndexSettingsChanged);
        return null;
    }

}
