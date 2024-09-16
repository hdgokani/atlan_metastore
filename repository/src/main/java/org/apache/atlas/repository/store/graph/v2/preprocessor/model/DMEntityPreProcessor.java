package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    private void createDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) {
        return;
    }


    private void updateDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(context.getUpdatedEntities())) {
            return;
        }

        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        long now = Instant.now().toEpochMilli();
        AtlasEntity.AtlasEntityWithExtInfo existingEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);
        AtlasRelatedObjectId existingModelVersion = (AtlasRelatedObjectId) existingEntity.getEntity().getRelationshipAttributes().get("dMVersion");
        List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) existingEntity.getEntity().getRelationshipAttributes().get("dMAttributes");

        if (existingModelVersion == null){
            throw new AtlasBaseException(AtlasErrorCode.MODEL_VERSION_NOT_EXIST);
        }


        // create modelVersion v2
        ModelResponse modelVersionResponse = replicateModelVersion(existingModelVersion, now);
        AtlasEntity existingModelVersionEntity = modelVersionResponse.getExistingEntity();
        AtlasEntity copyModelVersion = modelVersionResponse.getCopyEntity();
        AtlasVertex existingModelVersionVertex = modelVersionResponse.getExistingVertex();
        AtlasVertex copyModelVersionVertex = modelVersionResponse.getCopyVertex();
        String modelQualifiedName = (String) copyModelVersion.getAttribute(QUALIFIED_NAME);
//        System.out.println("copyModelVersion:  " +copyModelVersion.getGuid() + " existingModelVersion: "  +existingModelVersionEntity.getGuid());
//        System.out.println("copyModelVersionVertex:  " +copyModelVersionVertex.getId() + " existingModelVersionVertex: "  +existingModelVersionVertex.getId());

        // create entity e1'
        ModelResponse modelEntityResponse = replicateModelEntity(existingEntity.getEntity(), vertex, modelQualifiedName, now);
        AtlasVertex copyEntityVertex = modelEntityResponse.getCopyVertex();
        AtlasEntity copyEntity = modelEntityResponse.getCopyEntity();
        applyDiffs(entity, copyEntity);
//        System.out.println("copyEntity:  " +copyEntity.getGuid() + " existingEntity: "  +entity.getGuid());
//        System.out.println("copyModelVersionVertex:  " +copyModelVersionVertex.getId() + " existingModelVersionVertex: "  +existingModelVersionVertex.getId());


        // create modelVersion-modelEntity relationship
        createModelVersionModelEntityRelationship(copyModelVersionVertex, copyEntityVertex);

        //  create modelEntity-modelAttributeRelationship
        createModelEntityModelAttributeRelation(copyEntityVertex, existingEntityAttributes);

        // create model-modelVersion relation
        AtlasRelatedObjectId modelObject = createModelModelVersionRelation(existingModelVersionVertex, copyModelVersionVertex);


        /**
         * update context
         */
        // previousEntity and previousModelVersion
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(existingModelVersionEntity.getTypeName());

//        context.addUpdated(entity.getGuid(), entity,
//                entityType, vertex);
//
//        context.addUpdated(existingModelVersionEntity.getGuid(), existingModelVersionEntity,
//                modelVersionType, existingModelVersionVertex);

        // add createdEntity and createdModelVersion

        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(copyModelVersion.getGuid(), copyModelVersion, modelVersionType, copyModelVersionVertex);

        // remove existing entity from context so it is not updated
        context.removeUpdated(entity.getGuid(), entity,
                entityType, vertex);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        modelObject.getGuid(),
                        entityRetriever.getEntityVertex(modelObject.getGuid()));

//        for (AtlasRelatedObjectId attributeObject: existingEntityAttributes){
//            context.getDiscoveryContext().
//                    addResolvedGuid(
//                            attributeObject.getGuid(),
//                            entityRetriever.getEntityVertex(attributeObject.getGuid()));
//        }
        // add attributes as relationship is updated : check
    }


    private void applyDiffs(AtlasEntity sourceEntity, AtlasEntity destinationEntity) {
        RequestContext reqContext = RequestContext.get();
        AtlasEntity diffEntity = reqContext.getDifferentialEntity(sourceEntity.getGuid());
        if (!diffEntity.getTypeName().equals(ATLAS_DM_ENTITY_TYPE)){
            return;
        }
        replaceAttributes(destinationEntity.getAttributes(), diffEntity.getAttributes());
        replaceAttributes(destinationEntity.getRelationshipAttributes(), diffEntity.getRelationshipAttributes());
        replaceAttributes(destinationEntity.getAppendRelationshipAttributes(), diffEntity.getAppendRelationshipAttributes());
        replaceAttributes(destinationEntity.getRemoveRelationshipAttributes(), diffEntity.getRemoveRelationshipAttributes());
    }

}


