package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.CategoryPreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class DMEntityPreProcessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);


    public DMEntityPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
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
                createDMEntity(entity, vertex, context);
                // modelVersion --->modelName
                // entity ---> modelVersion
                // attribute ---> entityName

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
    }


    private void updateDMEntities(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMEntity(entity, vertex, context);

        // case when a mapping is added
        if (entity.getAppendRelationshipAttributes() != null) {
            LinkedHashMap<String, Object> appendAttributes = (LinkedHashMap<String, Object>) entity.getAppendRelationshipAttributes();
            ModelResponse modelResponseRelatedEntity = null;
            String guid = "";

            for (String attribute : appendAttributes.keySet()) {

                if (appendAttributes.get(attribute) instanceof List) {
                    List<LinkedHashMap<String, Object>> attributeList = (List<LinkedHashMap<String, Object>>) appendAttributes.get(attribute);

                    for (LinkedHashMap<String, Object> relationAttribute : attributeList) {
                        guid = (String) relationAttribute.get("guid");
                        if (Strings.isEmpty(guid)) {
                            continue;
                        }

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);

                        relationAttribute.put("guid", modelResponseRelatedEntity.getCopyEntity().getGuid());
                    }
                }

                if (appendAttributes.get(attribute) instanceof LinkedHashMap) {

                    LinkedHashMap<String, Object> attributeList = (LinkedHashMap<String, Object>) appendAttributes.get(attribute);
                    guid = (String) attributeList.get("guid");
                    if (!Strings.isEmpty(guid)) {

                        // update end2
                        modelResponseRelatedEntity = updateDMEntity(
                                entityRetriever.toAtlasEntity(guid),
                                entityRetriever.getEntityVertex(guid),
                                context);

                        attributeList.put("guid", modelResponseRelatedEntity.getCopyEntity().getGuid());
                    }
                }

            }

            modelResponseParentEntity.getCopyEntity().setAppendRelationshipAttributes(appendAttributes);
            context.removeUpdatedWithRelationshipAttributes(entity);
            context.setUpdatedWithRelationshipAttributes(modelResponseParentEntity.getCopyEntity());
        }
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

        return new ModelResponse(copyEntity, copyEntityVertex);
    }
}


