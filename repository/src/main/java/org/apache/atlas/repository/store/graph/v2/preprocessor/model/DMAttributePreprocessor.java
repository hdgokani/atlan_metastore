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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.repository.Constants.*;
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

        switch (operation) {
            case CREATE:
                break;
            case UPDATE:
                updateDMAttribute(entity, vertex, context);
        }
    }


    private void updateDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(ATLAS_DM_ATTRIBUTE_TYPE) ||
                CollectionUtils.isEmpty(context.getUpdatedEntities())) {
            return;
        }

        String attributeName = (String) entityAttribute.getAttribute(NAME);

        if (StringUtils.isEmpty(attributeName) || isNameInvalid(attributeName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        long now = Instant.now().toEpochMilli();

        AtlasEntity.AtlasEntityWithExtInfo existingEntityAttributeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertexAttribute, false);
        List<AtlasRelatedObjectId> existingEntityObjects = (List<AtlasRelatedObjectId>) existingEntityAttributeWithExtInfo.getEntity().getRelationshipAttributes().get("dMEntities");

        if (CollectionUtils.isEmpty(existingEntityObjects)) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_ENTITY_NOT_EXIST);
        }

        AtlasRelatedObjectId existingEntityObject = null;
        // retrieve entity where expiredAtBusinessDate and expiredAtSystemDate is not set
        // there will always be only one active entity
        for (AtlasRelatedObjectId _existingEntityObject : existingEntityObjects) {
            long expiredBusinessDate = (long) entityRetriever.toAtlasEntity(_existingEntityObject.getGuid()).getAttribute(ATLAS_DM_EXPIRED_AT_BUSINESS_DATE);
            long expiredSystemDate = (long) entityRetriever.toAtlasEntity(_existingEntityObject.getGuid()).getAttribute(ATLAS_DM_EXPIRED_AT_SYSTEM_DATE);
            if (expiredBusinessDate <= 0 && expiredSystemDate <= 0) {
                existingEntityObject = _existingEntityObject;
            }
        }

        if (existingEntityObject == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_ENTITY_NOT_EXIST);
        }


        AtlasEntity.AtlasEntityWithExtInfo existingEntityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingEntityObject.getGuid());
        AtlasRelatedObjectId existingModelVersionObject = (AtlasRelatedObjectId) existingEntityWithExtInfo.getEntity().getRelationshipAttributes().get("dMVersion");

        if (existingModelVersionObject == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_VERSION_NOT_EXIST);
        }
        AtlasEntity.AtlasEntityWithExtInfo existingEntityVersionWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionObject.getGuid());
        AtlasRelatedObjectId existingModelObject = (AtlasRelatedObjectId) existingEntityVersionWithExtInfo.getEntity().getRelationshipAttributes().get("dMModel");

        if (existingModelObject == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
        }

        // create modelVersion v1 ---> v2
        ModelResponse modelVersionResponse = replicateModelVersion(existingModelVersionObject, now);
        AtlasEntity existingModelVersionEntity = modelVersionResponse.getExistingEntity();
        AtlasEntity copyModelVersion = modelVersionResponse.getCopyEntity();
        AtlasVertex existingModelVersionVertex = modelVersionResponse.getExistingVertex();
        AtlasVertex copyModelVersionVertex = modelVersionResponse.getCopyVertex();
       // String modelVersionQualifiedName = (String) copyModelVersion.getAttribute(QUALIFIED_NAME);

        // retrieve active entities linked to previous version
        existingEntityObjects = (List<AtlasRelatedObjectId>) existingModelVersionEntity.getRelationshipAttributes().get("dMEntities");

        List<AtlasEntity> activeEntities = new ArrayList<>();
        for (AtlasRelatedObjectId _existingEntityObject : existingEntityObjects) {
            AtlasEntity entity = entityRetriever.toAtlasEntity(_existingEntityObject.getGuid());
            if ((long) entity.getAttributes().get(ATLAS_DM_EXPIRED_AT_BUSINESS_DATE) > 0 && (long) entity.getAttributes().get(ATLAS_DM_EXPIRED_AT_SYSTEM_DATE) > 0) {
               continue;
            }
            activeEntities.add(entity);
        }


        // ex: default/dm/1234/modelName/entityName/attributeName
        String qualifiedAttributeNamePrefix = (String) entityAttribute.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = qualifiedAttributeNamePrefix.lastIndexOf("/");

        // ex: default/dm/1234/modelName/entityName
        String qualifiedEntityNamePrefix = qualifiedAttributeNamePrefix.substring(0, lastIndex);


        // create entity e1 ---> e1'
        ModelResponse modelEntityResponse = replicateModelEntity(
                existingEntityWithExtInfo.getEntity(),
                entityRetriever.getEntityVertex(existingModelVersionEntity.getGuid()),
                qualifiedEntityNamePrefix,
                now);
        AtlasEntity existingEntity= modelEntityResponse.getExistingEntity();
        AtlasVertex existingEntityVertex = modelEntityResponse.getExistingVertex();
        AtlasVertex copyEntityVertex = modelEntityResponse.getCopyVertex();
        AtlasEntity copyEntity = modelEntityResponse.getCopyEntity();
        List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) modelEntityResponse.getExistingEntity().getRelationshipAttributes().get("dMAttributes");


        // create attribute a1 ---> a1'
        ModelResponse modelAttributeResponse = replicateModelAttribute(
                existingEntityAttributeWithExtInfo.getEntity(),
                entityRetriever.getEntityVertex(entityAttribute.getGuid()),
                qualifiedAttributeNamePrefix,
                now);

        AtlasVertex copyAttributeVertex = modelAttributeResponse.getCopyVertex();
        AtlasEntity copyAttribute = modelAttributeResponse.getCopyEntity();
        applyDiffs(entityAttribute, copyAttribute, ATLAS_DM_ATTRIBUTE_TYPE);
        unsetExpiredDates(copyAttribute, copyAttributeVertex);

        // create model-modelVersion relationship
        AtlasRelatedObjectId modelObject = createModelModelVersionRelation(existingModelVersionVertex, copyModelVersionVertex);

        // create modelVersion-entity relationship [with new entity]
        createModelVersionModelEntityRelationship(copyModelVersionVertex, copyEntityVertex);

        // create modelVersion-entity relationship [with existing entities]
        for (AtlasEntity entity : activeEntities) {
            createModelVersionModelEntityRelationship(copyModelVersionVertex, entityRetriever.getEntityVertex(entity.getGuid()));
        }

        // create entity - attribute relation [with new attribute]
        createModelEntityModelAttributeRelation(copyEntityVertex, copyAttributeVertex);

        // create entity- attribute relation [with existing attributes]
        createModelEntityModelAttributeRelation(copyEntityVertex, existingEntityAttributes);

        AtlasEntityType attributeType = typeRegistry.getEntityTypeByName(entityAttribute.getTypeName());
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(copyEntity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(existingModelVersionEntity.getTypeName());

        context.addCreated(copyAttribute.getGuid(), copyAttribute, attributeType, copyAttributeVertex);
        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(copyModelVersion.getGuid(), copyModelVersion, modelVersionType, copyModelVersionVertex);


        context.addUpdated(existingModelVersionEntity.getGuid(), existingModelVersionEntity,
                modelVersionType, existingModelVersionVertex);
        context.addUpdated(existingEntity.getGuid(), existingEntity,
                entityType, existingEntityVertex);
        // remove existing entity from context so it is not updated
        context.removeUpdated(entityAttribute.getGuid(), entityAttribute,
                entityType, vertexAttribute);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelObject.getGuid(),
                        entityRetriever.getEntityVertex(modelObject.getGuid()));
    }

}