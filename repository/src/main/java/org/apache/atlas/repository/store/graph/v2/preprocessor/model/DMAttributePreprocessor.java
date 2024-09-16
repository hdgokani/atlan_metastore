package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class DMAttributePreprocessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);


    public DMAttributePreprocessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        super(typeRegistry, entityRetriever, entityGraphMapper, atlasRelationshipStore);
    }


    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("ModelPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation){
            case CREATE:
                break;
            case UPDATE:
        }
    }

    private void updateDMAttribute(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        long now = Instant.now().toEpochMilli();

        AtlasEntity.AtlasEntityWithExtInfo existingEntityAttributeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);
        AtlasRelatedObjectId existingEntityObject = (AtlasRelatedObjectId) existingEntityAttributeWithExtInfo.getEntity().getRelationshipAttributes().get("dMEntity");
        AtlasEntity.AtlasEntityWithExtInfo existingEntityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingEntityObject.getGuid());
        AtlasRelatedObjectId existingModelVersionObject = (AtlasRelatedObjectId) existingEntityWithExtInfo.getEntity().getRelationshipAttributes().get("dMVersion");
        AtlasEntity.AtlasEntityWithExtInfo existingEntityVersionWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionObject.getGuid());
        AtlasRelatedObjectId existingModelObject= (AtlasRelatedObjectId) existingEntityVersionWithExtInfo.getEntity().getRelationshipAttributes().get("dMModel");


        // create Model version
        ModelResponse modelVersionResponse = replicateModelVersion(existingModelVersionObject, now);
        AtlasEntity existingModelVersionEntity = modelVersionResponse.getExistingEntity();
        AtlasEntity copyModelVersion = modelVersionResponse.getCopyEntity();
        AtlasVertex existingModelVersionVertex = modelVersionResponse.getExistingVertex();
        AtlasVertex copyModelVersionVertex = modelVersionResponse.getCopyVertex();
        String modelQualifiedName = (String) copyModelVersion.getAttribute(QUALIFIED_NAME);
    }

}