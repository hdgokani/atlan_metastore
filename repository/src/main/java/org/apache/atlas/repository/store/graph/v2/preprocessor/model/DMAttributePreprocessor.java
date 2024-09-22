package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
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
import java.util.*;

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
                createDMAttribute(entity, vertex, context);
                break;
            case UPDATE:
                updateDMAttribute(entity, vertex, context);
        }
    }

    private void createDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(ATLAS_DM_ATTRIBUTE_TYPE)) {
            return;
        }
        if (CollectionUtils.isEmpty(context.getCreatedEntities())) {
            return;
        }

        String entityName = (String) entityAttribute.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        long now = RequestContext.get().getRequestTime();


        // get entity qualifiedName with qualifiedNamePrefix
        String attributeQualifiedNamePrefix = (String) entityAttribute.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = attributeQualifiedNamePrefix.lastIndexOf("/");
        String entityQualifiedNamePrefix = attributeQualifiedNamePrefix.substring(0, lastIndex);
        String namespace = (String) entityAttribute.getAttributes().get(ATLAS_DM_NAMESPACE);
        String modelVersion = "v2";

        ModelResponse modelENtityResponse = null;
        AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(ATLAS_DM_ENTITY_TYPE, entityQualifiedNamePrefix);
        lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");

        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);

        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);
        String modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL),
                attrValues);

        List<AtlasRelatedObjectId> existingAttributes = null;

        if (latestEntityVertex != null) {
            modelENtityResponse = replicateModelEntity(
                    entityRetriever.toAtlasEntity(latestEntityVertex),
                    latestEntityVertex,
                    entityQualifiedNamePrefix,
                    now
            );

            if (modelENtityResponse.getExistingEntity() != null && modelENtityResponse.getExistingEntity().getRelationshipAttributes() != null) {
                existingAttributes = (List<AtlasRelatedObjectId>) modelENtityResponse.getExistingEntity().getRelationshipAttributes().get("dMAttributes");
            }
        } else {
            modelENtityResponse = createEntity(
                    attributeQualifiedNamePrefix + "_" + now,
                    ATLAS_DM_ATTRIBUTE_TYPE,
                    namespace,
                    context
            );
        }

        List<AtlasRelatedObjectId> existingEntities = null;
        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);
        if (modelVersionResponse.getCopyEntity() == null) {
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + modelVersion),
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }
        AtlasEntity latestModelVersionEntity = modelVersionResponse.getCopyEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getCopyVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getCopyVertex());

        // modelVersion --- entitiesOfExistingModelVersion
        if (modelVersionResponse.getExistingEntity() != null && modelVersionResponse.getExistingEntity().getRelationshipAttributes() != null) {
            existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity().getRelationshipAttributes().get("dMEntities");
            createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);
        }

        // entity --- attributes of existingEntity relation
        createModelEntityModelAttributeRelation(modelENtityResponse.getCopyVertex(), existingAttributes);

        // latest entity ---- new attribute relation
        createModelEntityModelAttributeRelation(modelENtityResponse.getCopyVertex(), vertexAttribute);

        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), latestModelVersionVertex);

        context.addCreated(modelENtityResponse.getCopyEntity().getGuid(), modelENtityResponse.getCopyEntity(),
                typeRegistry.getEntityTypeByName(ATLAS_DM_ENTITY_TYPE), modelENtityResponse.getCopyVertex());

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

    }

    private void updateDMAttribute(AtlasEntity entityAttribute, AtlasVertex vertexAttribute, EntityMutationContext context) throws AtlasBaseException {
        if (!entityAttribute.getTypeName().equals(ATLAS_DM_ATTRIBUTE_TYPE)) {
            return;
        }

        String attributeName = (String) entityAttribute.getAttribute(NAME);

        if (StringUtils.isEmpty(attributeName) || isNameInvalid(attributeName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        long now = RequestContext.get().getRequestTime();


        // get entity qualifiedName with qualifiedNamePrefix
        String attributeQualifiedNamePrefix = (String) entityAttribute.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = attributeQualifiedNamePrefix.lastIndexOf("/");
        String entityQualifiedNamePrefix = attributeQualifiedNamePrefix.substring(0, lastIndex);
        String namespace = (String) entityAttribute.getAttributes().get(ATLAS_DM_NAMESPACE);
        String modelVersion = "v2";

        ModelResponse modelENtityResponse = null;
        AtlasVertex latestEntityVertex = AtlasGraphUtilsV2.findLatestEntityAttributeVerticesByType(ATLAS_DM_ENTITY_TYPE, entityQualifiedNamePrefix);

        // get model qualifiedName with qualifiedNamePrefix
        lastIndex = entityQualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = entityQualifiedNamePrefix.substring(0, lastIndex);
        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);

        String modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL),
                attrValues);

        List<AtlasRelatedObjectId> existingAttributes = null;

        if (latestEntityVertex != null) {
            modelENtityResponse = replicateModelEntity(
                    entityRetriever.toAtlasEntity(latestEntityVertex),
                    latestEntityVertex,
                    entityQualifiedNamePrefix,
                    now
            );
            if (modelENtityResponse.getExistingEntity()!=null && modelENtityResponse.getExistingEntity().getRelationshipAttributes()!=null){
                existingAttributes = (List<AtlasRelatedObjectId>) modelENtityResponse.getExistingEntity().getAttributes().get("dMAttributes");
            }
        } else {
            modelENtityResponse = createEntity(
                    attributeQualifiedNamePrefix + "_" + now,
                    ATLAS_DM_ATTRIBUTE_TYPE,
                    namespace,
                    context
            );
        }

        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

        if (modelVersionResponse.getCopyEntity() == null) {
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + modelVersion),
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }
        AtlasEntity latestModelVersionEntity = modelVersionResponse.getCopyEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getCopyVertex();

        List<AtlasRelatedObjectId> existingEntities = null;

        if (modelVersionResponse.getExistingEntity() != null && modelVersionResponse.getExistingEntity().getRelationshipAttributes() != null) {
            existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity().getRelationshipAttributes().get("dMEntities");
        }

        AtlasEntity existingEntityAttributeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(entityAttribute.getGuid(), false).getEntity();

        // create attribute a1 ---> a1'
        ModelResponse modelAttributeResponse = replicateModelAttribute(
                existingEntityAttributeWithExtInfo,
                entityRetriever.getEntityVertex(entityAttribute.getGuid()),
                attributeQualifiedNamePrefix,
                now);

        AtlasVertex copyAttributeVertex = modelAttributeResponse.getCopyVertex();
        AtlasEntity copyAttribute = modelAttributeResponse.getCopyEntity();
        applyDiffs(entityAttribute, copyAttribute, ATLAS_DM_ATTRIBUTE_TYPE);
        unsetExpiredDates(copyAttribute, copyAttributeVertex);

        // create model-modelVersion relationship
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // create modelVersion-entity relationship [with new entity]
        createModelVersionModelEntityRelationship(latestModelVersionVertex, modelENtityResponse.getCopyVertex());

        // create modelVersion-entity relationship [with existing entities]
        createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);


        // create entity - attribute relation [with new attribute]
        createModelEntityModelAttributeRelation(modelENtityResponse.getCopyVertex(), copyAttributeVertex);

        // create entity - attribute relation [with existing attributes]
        createModelEntityModelAttributeRelation(modelENtityResponse.getCopyVertex(), existingAttributes);

        AtlasEntityType attributeType = typeRegistry.getEntityTypeByName(entityAttribute.getTypeName());
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(modelENtityResponse.getCopyEntity().getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(latestModelVersionEntity.getTypeName());

        context.addCreated(copyAttribute.getGuid(), copyAttribute, attributeType, copyAttributeVertex);
        context.addCreated(modelENtityResponse.getCopyEntity().getGuid(), modelENtityResponse.getCopyEntity(),
                entityType, modelENtityResponse.getCopyVertex());
        context.addCreated(latestModelVersionEntity.getGuid(),
                latestModelVersionEntity, modelVersionType, latestModelVersionVertex);

        context.removeUpdated(entityAttribute.getGuid(), entityAttribute,
                entityType, vertexAttribute);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));
    }
}