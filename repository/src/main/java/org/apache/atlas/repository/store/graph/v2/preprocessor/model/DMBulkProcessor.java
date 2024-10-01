package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;

public class DMEntityBulkProcessor extends AbstractModelPreProcessor {
    public DMEntityBulkProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        super(typeRegistry, entityRetriever, entityGraphMapper, atlasRelationshipStore);
    }

    @Override
    public void processAttributes(AtlasStruct entity, EntityMutationContext context, EntityMutations.EntityOperation operation) throws AtlasBaseException {
        return;
    }

    public void
}
