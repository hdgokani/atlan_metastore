package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import com.esotericsoftware.minlog.Log;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

@Component
public class DMEntityPreProcessor extends AbstractModelPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DMEntityPreProcessor.class);


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

        String modelGuid = context.getModel(modelQualifiedName);
        LOG.info("model retrieved from cache: " + StringUtils.isNotEmpty(modelGuid));

        if (StringUtils.isEmpty(modelGuid)) {
            Map<String, Object> attrValues = new HashMap<>();
            attrValues.put(QUALIFIED_NAME, modelQualifiedName);
            modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                    typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL),
                    attrValues);

            if (StringUtils.isEmpty(modelGuid)){
                throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_NOT_EXIST);
            }
            context.cacheModel(modelQualifiedName, modelGuid);
            Log.info("cached model", modelQualifiedName);
        }

        ModelResponse modelVersionResponse = context.getModelVersion(modelQualifiedName);
        LOG.info("model version retrieved from cache: " + (modelVersionResponse != null));

        if (modelVersionResponse == null) {
            modelVersionResponse = replicateModelVersion(modelGuid, modelQualifiedName, now);

            // create modelVersion
            if (modelVersionResponse.getReplicaEntity() == null) {
                String namespace = (String) entity.getAttributes().get(ATLAS_DM_NAMESPACE);
                modelVersionResponse = createEntity(
                        (modelQualifiedName + "/" + "v1"),
                        "v1",
                        ATLAS_DM_VERSION_TYPE,
                        namespace,
                        context);
            }

            // model --- modelVersion relation
            createModelModelVersionRelation(modelGuid, modelVersionResponse.getReplicaEntity().getGuid());
            context.cacheModelVersion(modelQualifiedName, modelVersionResponse);
            Log.info("cached model version", modelQualifiedName);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getReplicaEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getReplicaVertex();


        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, vertex);

        // modelVersion --- entitiesOfExistingModelVersion
        if (modelVersionResponse.getExistingEntity() != null && MapUtils.isNotEmpty(modelVersionResponse.getExistingEntity().getRelationshipAttributes())) {
            List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) modelVersionResponse.getExistingEntity()
                            .getRelationshipAttributes()
                            .get("dMEntities");
            createModelVersionModelEntityRelationship(latestModelVersionVertex, entity, existingEntities, context);

        }

        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE);

        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
                modelVersionType, latestModelVersionVertex);

        if (modelVersionResponse.getExistingEntity() != null) {
            context.removeUpdated(modelVersionResponse.getExistingEntity().getGuid(),
                    modelVersionResponse.getExistingEntity(), modelVersionType,
                    modelVersionResponse.getExistingVertex());
            context.addUpdated(modelVersionResponse.getExistingEntity().getGuid(),
                    modelVersionResponse.getExistingEntity(), modelVersionType,
                    modelVersionResponse.getExistingVertex());
        }
        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));

        entity.setRelationshipAttributes(
                processRelationshipAttributesForEntity(entity, entity.getRelationshipAttributes(), context));
    }

    private void updateDMEntities(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        ModelResponse modelResponseParentEntity = updateDMEntity(entity, vertex, context);

        if (entity.getRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entity, entity.getRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setRelationshipAttributes(appendRelationshipAttributes);
        }

        // case when a mapping is added
        if (entity.getAppendRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entity, entity.getAppendRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setAppendRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithRelationshipAttributes(entity);
            context.setUpdatedWithRelationshipAttributes(modelResponseParentEntity.getReplicaEntity());
        }

        if (entity.getRemoveRelationshipAttributes() != null) {
            Map<String, Object> appendRelationshipAttributes = processRelationshipAttributesForEntity(entity, entity.getRemoveRelationshipAttributes(), context);
            modelResponseParentEntity.getReplicaEntity().setRemoveRelationshipAttributes(appendRelationshipAttributes);
            context.removeUpdatedWithDeleteRelationshipAttributes(entity);
            context.setUpdatedWithRemoveRelationshipAttributes(modelResponseParentEntity.getReplicaEntity());
        }
    }
}



