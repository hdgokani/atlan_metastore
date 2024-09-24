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
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
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

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class DMEntityPreProcessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);
    Set<String> allowedRelationshipNames;


    public DMEntityPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        super(typeRegistry, entityRetriever, entityGraphMapper, atlasRelationshipStore);
        allowedRelationshipNames = new HashSet<>();
        allowedRelationshipNames.add("dMMappedToEntities");
        allowedRelationshipNames.add("dMMappedFromEntities");
        allowedRelationshipNames.add("dMRelatedFromEntities");
        allowedRelationshipNames.add("dMRelatedToEntities");
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
                createDMEntity(entity, vertex, context);
                break;
            case UPDATE:
                updateDMEntities(entity, vertex, context);
                break;
        }
    }

    private void createDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(ATLAS_DM_ENTITY_TYPE)) {
            return;
        }
        if (CollectionUtils.isEmpty(context.getCreatedEntities())) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }
        long now = RequestContext.get().getRequestTime();

        // get model qualifiedName with qualifiedNamePrefix
        String qualifiedNamePrefix = (String) entity.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = qualifiedNamePrefix.substring(0, lastIndex);

        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);

        AtlasVertex modelVertex = AtlasGraphUtilsV2.findByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL), attrValues);

        if (modelVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
        }

        String modelGuid = AtlasGraphUtilsV2.getIdFromVertex(modelVertex);
        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

        if (modelVersionResponse.getCopyEntity() == null) {
            String namespace = (String) entity.getAttributes().get(ATLAS_DM_NAMESPACE);
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + "v1"),
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getCopyEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getCopyVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, vertex);

        if (modelVersionResponse.getExistingEntity() != null) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity()
                    .getRelationshipAttributes()
                    .get("dMEntities");
            // modelVersion --- entitiesOfExistingModelVersion
            createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);

        }
        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), latestModelVersionVertex);
        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        /**
         * Optimise this : validate why reference is not created in legacy code
         */
        resolveReferences(entity, context);
    }

    private void updateDMEntities(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMEntity(entity, vertex, context);

        // case when a mapping is added
        if (entity.getAppendRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processAppendRemoveRelationshipAttributes(entity, entity.getAppendRelationshipAttributes(), context);
            modelResponseParentEntity.getCopyEntity().setAppendRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithRelationshipAttributes(entity);
            context.setUpdatedWithRelationshipAttributes(modelResponseParentEntity.getCopyEntity());
        }

        if (entity.getRemoveRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processAppendRemoveRelationshipAttributes(entity, entity.getAppendRelationshipAttributes(), context);
            modelResponseParentEntity.getCopyEntity().setRemoveRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithDeleteRelationshipAttributes(entity);
            context.setUpdatedWithRemoveRelationshipAttributes(modelResponseParentEntity.getCopyEntity());
        }
    }

    private Map<String, Object> processAppendRemoveRelationshipAttributes(AtlasEntity entity, Map<String, Object> relationshipAttributes, EntityMutationContext context) throws AtlasBaseException {
        Map<String, Object> appendAttributesDestination = new HashMap<>();
        if (relationshipAttributes != null) {
            Map<String, Object> appendAttributesSource = (Map<String, Object>) relationshipAttributes;
            ;
            ModelResponse modelResponseRelatedEntity = null;
            String guid = "";
            Set<String> allowedRelations = allowedRelationshipsForEntityType(entity.getTypeName());

            for (String attribute : appendAttributesSource.keySet()) {

                if (appendAttributesSource.get(attribute) instanceof List) {

                    if (!allowedRelations.contains(attribute)) {
                        continue;
                    }
                    List<Map<String, Object>> destList = new ArrayList<>();
                    Map<String, Object> destMap = null;

                    List<Map<String, Object>> attributeList = (List<Map<String, Object>>) appendAttributesSource.get(attribute);

                    for (Map<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);
                        //relationAttribute.put("guid", modelResponseRelatedEntity.getCopyEntity());
                        destMap = new HashMap<>(relationAttribute);
                        guid = modelResponseRelatedEntity.getCopyEntity().getGuid();
                        destMap.put("guid", guid);
                        //destMap.put(QUALIFIED_NAME, )
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getCopyVertex());
                        destList.add(destMap);
                    }
                    appendAttributesDestination.put(attribute, destList);
                } else {
                    if (appendAttributesSource.get(attribute) instanceof Map) {
                        LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributesSource.get(attribute);
                        guid = (String) attributeList.get("guid");

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);

                        Map<String, Object> destMap = new HashMap<>(attributeList);
                        destMap.put("guid", guid);
                        guid = modelResponseRelatedEntity.getCopyEntity().getGuid();
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getCopyVertex());
                        appendAttributesDestination.put(attribute, destMap);
                    }
                }
            }
        }
        return appendAttributesDestination;
    }

    private ModelResponse updateDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (!entity.getTypeName().equals(ATLAS_DM_ENTITY_TYPE)) {
            return new ModelResponse(entity, vertex);
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }


        long now = RequestContext.get().getRequestTime();
        AtlasEntity.AtlasEntityWithExtInfo existingEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);
        List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) existingEntity.getEntity().getRelationshipAttributes().get("dMAttributes");


        // get model qualifiedName with qualifiedNamePrefix
        String qualifiedNamePrefix = (String) entity.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);
        int lastIndex = qualifiedNamePrefix.lastIndexOf("/");
        String modelQualifiedName = qualifiedNamePrefix.substring(0, lastIndex);
        String modelVersion = "v1";


        Map<String, Object> attrValues = new HashMap<>();
        attrValues.put(QUALIFIED_NAME, modelQualifiedName);

        AtlasVertex modelVertex = AtlasGraphUtilsV2.findByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL), attrValues);

        String modelGuid = AtlasGraphUtilsV2.getIdFromVertex(modelVertex);
        ModelResponse modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

        // model is not replicated successfully
        if (modelVersionResponse.getCopyEntity() == null) {
            String namespace = (String) entity.getAttributes().get(ATLAS_DM_NAMESPACE);
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + modelVersion),
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getCopyEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getCopyVertex();
        AtlasEntity existingVersion = modelVersionResponse.getExistingEntity();

        // create entity e1 ---> e1'
        ModelResponse modelEntityResponse = replicateModelEntity(existingEntity.getEntity(), vertex, qualifiedNamePrefix, now);
        AtlasVertex copyEntityVertex = modelEntityResponse.getCopyVertex();
        AtlasEntity copyEntity = modelEntityResponse.getCopyEntity();
        applyDiffs(entity, copyEntity, ATLAS_DM_ENTITY_TYPE);
        unsetExpiredDates(copyEntity, copyEntityVertex);

        // create model-modelVersion relation
        createModelModelVersionRelation(modelGuid, latestModelVersionEntity.getGuid());

        // create modelVersion-modelEntity relationship with new entity
        createModelVersionModelEntityRelationship(latestModelVersionVertex, copyEntityVertex);

        // create modelVersion-modelEntity relation with old entities which are not expired
        if (existingVersion != null) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) existingVersion.getRelationshipAttributes().get("dMEntities");
            createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);
        }
        //  create modelEntity-modelAttributeRelationship
        createModelEntityModelAttributeRelation(copyEntityVertex, existingEntityAttributes);


        /**
         * update context
         */
        // previousEntity and previousModelVersion
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(modelVersionResponse.getCopyEntity().getTypeName());

        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity, modelVersionType, latestModelVersionVertex);

        //remove existing entity from context so it is not updated
        context.removeUpdated(entity.getGuid(), entity,
                entityType, vertex);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        modelVertex);

        /***
         * debug why its already not there in context
         */
        resolveReferences(entity, context);

        return new ModelResponse(copyEntity, copyEntityVertex);
    }

    private void resolveReferences(AtlasEntity entity, EntityMutationContext context) {
        if (entity.getRelationshipAttributes() != null) {
            Map<String, Object> appendAttributesSource = (Map<String, Object>) entity.getRelationshipAttributes();
            ModelResponse modelResponseRelatedEntity = null;
            String guid = "";
            Set<String> allowedRelations = allowedRelationshipsForEntityType(entity.getTypeName());

            for (String attribute : appendAttributesSource.keySet()) {

                if (appendAttributesSource.get(attribute) instanceof List) {

                    if (!allowedRelations.contains(attribute)) {
                        continue;
                    }
                    List<Map<String, Object>> attributeList = (List<Map<String, Object>>) appendAttributesSource.get(attribute);

                    for (Map<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getCopyVertex());
                    }
                } else {
                    if (appendAttributesSource.get(attribute) instanceof Map) {
                        LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributesSource.get(attribute);
                        guid = (String) attributeList.get("guid");
                        context.getDiscoveryContext().addResolvedGuid(guid, modelResponseRelatedEntity.getCopyVertex());
                    }
                }
            }
        }

    }
}



