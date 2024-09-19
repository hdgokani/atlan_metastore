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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
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
                updateDMEntity(entity, vertex, context);
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

        String modelGuid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(
                typeRegistry.getEntityTypeByName(ATLAS_DM_DATA_MODEL),
                (Map<String, Object>) new HashMap<String, Object>().put(QUALIFIED_NAME, modelQualifiedName));

        AtlasEntity.AtlasEntityWithExtInfo dataModel = entityRetriever.toAtlasEntityWithExtInfo(modelGuid, false);
        List<AtlasRelatedObjectId> existingModelVersions = (List<AtlasRelatedObjectId>) dataModel.getEntity().getAttributes().get("dMVersions");

        String modelVersion = "v1";
        AtlasRelatedObjectId existingModelVersionObj = null;

        if (CollectionUtils.isNotEmpty(existingModelVersions)) {
            int existingVersionNumber = existingModelVersions.size();
            modelVersion = "v" + (++existingVersionNumber);

            for (AtlasRelatedObjectId modelVersionObj : existingModelVersions) {
                if (((int) modelVersionObj.getAttributes().get(ATLAS_DM_BUSINESS_DATE) > 0) ||
                        ((int) modelVersionObj.getAttributes().get(ATLAS_DM_SYSTEM_DATE) > 0)) {
                    continue;
                }
                existingModelVersionObj = modelVersionObj;
            }
        }

        AtlasEntity existingModelVersionEntity = null;
        AtlasVertex existingModelVersionVertex = null;
        List<AtlasRelatedObjectId> existingEntities = null;
        ModelResponse modelVersionResponse = null;


        if (existingModelVersionObj != null) {
            existingModelVersionEntity = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionObj.getGuid()).getEntity();
            existingModelVersionVertex = entityRetriever.getEntityVertex(existingModelVersionObj.getGuid());
            setModelExpiredAtDates(existingModelVersionEntity, existingModelVersionVertex, now);
            existingEntities= (List<AtlasRelatedObjectId>) existingModelVersionEntity.getRelationshipAttributes().get("dMEntities");
            modelVersionResponse = replicateModelVersion(existingModelVersionObj, now);
        } else {
            String namespace = (String) entity.getAttributes().get(ATLAS_DM_NAMESPACE);
            modelVersionResponse = createEntity(
                    (modelQualifiedName + "/" + modelVersion),
                    ATLAS_DM_VERSION_TYPE,
                    namespace,
                    context);
        }

        AtlasEntity latestModelVersionEntity = modelVersionResponse.getCopyEntity();
        AtlasVertex latestModelVersionVertex = modelVersionResponse.getCopyVertex();


        // model --- modelVersion relation
        createModelModelVersionRelation(modelGuid,latestModelVersionEntity.getGuid());

        // modelVersion --- entity relation
        createModelVersionModelEntityRelationship(latestModelVersionVertex, vertex);

        // modelVersion --- entitiesOfExistingModelVersion
        createModelVersionModelEntityRelationship(latestModelVersionVertex, existingEntities);


        context.addCreated(latestModelVersionEntity.getGuid(), latestModelVersionEntity,
         typeRegistry.getEntityTypeByName(ATLAS_DM_VERSION_TYPE), latestModelVersionVertex);
        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelGuid,
                        entityRetriever.getEntityVertex(modelGuid));
    }


    private void updateDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
       // lets say entity has attribute : versionQualifiedName
        // default/dm/1725359500/com.jpmc.ct.fri/RegulatoryReporting/entity1/epoch
        // query with qualifiedName : default/dm/1725359500/com.jpmc.ct.fri/RegulatoryReporting/entity1

        if (!entity.getTypeName().equals(ATLAS_DM_ENTITY_TYPE)) {
            return;
        }
        if (CollectionUtils.isEmpty(context.getUpdatedEntities())) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }


        long now = RequestContext.get().getRequestTime();
        AtlasEntity.AtlasEntityWithExtInfo existingEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);
        AtlasRelatedObjectId existingModelVersion = (AtlasRelatedObjectId) existingEntity.getEntity().getRelationshipAttributes().get("dMVersions");
        List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) existingEntity.getEntity().getRelationshipAttributes().get("dMAttributes");

        if (existingModelVersion == null) {
            throw new AtlasBaseException(AtlasErrorCode.DATA_MODEL_VERSION_NOT_EXIST, existingModelVersion.getGuid());
        }

        // create modelVersion v1 ---> v2
        ModelResponse modelVersionResponse = replicateModelVersion(existingModelVersion, now);
        AtlasEntity existingModelVersionEntity = modelVersionResponse.getExistingEntity();
        AtlasEntity copyModelVersion = modelVersionResponse.getCopyEntity();
        AtlasVertex existingModelVersionVertex = modelVersionResponse.getExistingVertex();
        AtlasVertex copyModelVersionVertex = modelVersionResponse.getCopyVertex();
       // String modelVersionAttributeQualifiedName = (String) copyModelVersion.getAttribute(QUALIFIED_NAME);
        List<AtlasRelatedObjectId> existingEntities = (List<AtlasRelatedObjectId>) existingModelVersionEntity.getRelationshipAttributes().get("dMEntities");

        String entityQualifiedNamePrefix = (String) entity.getAttributes().get(ATLAS_DM_QUALIFIED_NAME_PREFIX);

        // create entity e1 ---> e1'
        ModelResponse modelEntityResponse = replicateModelEntity(existingEntity.getEntity(), vertex, entityQualifiedNamePrefix, now);
        AtlasVertex copyEntityVertex = modelEntityResponse.getCopyVertex();
        AtlasEntity copyEntity = modelEntityResponse.getCopyEntity();
        applyDiffs(entity, copyEntity, ATLAS_DM_ENTITY_TYPE);
        unsetExpiredDates(copyEntity, copyEntityVertex);

        // create model-modelVersion relation
        AtlasRelatedObjectId modelObject = createModelModelVersionRelation(existingModelVersionVertex, copyModelVersionVertex);

        // create modelVersion-modelEntity relationship with new entity
        createModelVersionModelEntityRelationship(copyModelVersionVertex, copyEntityVertex);

        // create modelVersion-modelEntity relation with old entities which are not expired
        createModelVersionModelEntityRelationship(copyModelVersionVertex, existingEntities);

        //  create modelEntity-modelAttributeRelationship
        createModelEntityModelAttributeRelation(copyEntityVertex, existingEntityAttributes);


        /**
         * update context
         */
        // previousEntity and previousModelVersion
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(existingModelVersionEntity.getTypeName());

        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(copyModelVersion.getGuid(), copyModelVersion, modelVersionType, copyModelVersionVertex);

        context.addUpdated(entity.getGuid(), entity, entityType, vertex);
        context.addUpdated(existingModelVersionEntity.getGuid(), existingModelVersionEntity,
                modelVersionType, existingModelVersionVertex );

        // remove existing entity from context so it is not updated
        context.removeUpdated(entity.getGuid(), entity,
                entityType, vertex);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelObject.getGuid(),
                        entityRetriever.getEntityVertex(modelObject.getGuid()));

    }
}


