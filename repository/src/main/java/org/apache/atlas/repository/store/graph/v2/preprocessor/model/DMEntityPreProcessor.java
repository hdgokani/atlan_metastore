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
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.formula.functions.Na;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.isNameInvalid;

public class ModelPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);
    private static final String ENTITY_TYPE = "DMEntity";
    private static final String ATTRIBUTE_TYPE = "DMAttribute";
    protected final AtlasTypeRegistry typeRegistry;
    protected final EntityGraphRetriever entityRetriever;
    protected EntityGraphMapper entityGraphMapper;
    protected EntityDiscoveryService discovery;
    protected AtlasRelationshipStore atlasRelationshipStore;

    public ModelPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, EntityDiscoveryService discovery, AtlasRelationshipStore atlasRelationshipStore) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityGraphMapper = entityGraphMapper;
        this.discovery = discovery;
        this.atlasRelationshipStore = atlasRelationshipStore;
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
                processCreateModel(entity, vertex, context);
                // modelVersion --->modelName
                // entity ---> modelVersion
                // attribute ---> entityName

                break;
            case UPDATE:
                processUpdateModel(entity, vertex, context);

                break;
        }
    }

    private void processCreateModel(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) {
        return;
    }

    private void processUpdateModel(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateMOdel");
        /***
         *
         * 1. if entity is updated
         * create deep copy of entity , e1' set expiredAtBusinessDate and expiredAtSystemDate
         * get relationshipEdges of e1: a1,a1
         * create relationship a1, a2 to e1'
         * get modelVersion from modelVersionQualifiedName in entity
         * deep copy model version and name to v2
         * create relationship v2--e1'
         *
         *
         *
         *
         *
         */
        switch (entity.getTypeName()) {
            case ENTITY_TYPE:
                updateDMEntity(entity, vertex, context);
                break;
            case ATTRIBUTE_TYPE:
                break;
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updateDMEntity(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        String entityName = (String) entity.getAttribute(NAME);

        if (StringUtils.isEmpty(entityName) || isNameInvalid(entityName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        long now = Instant.now().toEpochMilli();

        AtlasEntity.AtlasEntityWithExtInfo existingEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex, false);
        AtlasRelatedObjectId existingModelVersion = (AtlasRelatedObjectId) existingEntity.getEntity().getRelationshipAttributes().get("dMVersion");
        List<AtlasRelatedObjectId> existingEntityAttributes = (List<AtlasRelatedObjectId>) existingEntity.getEntity().getRelationshipAttributes().get("dMAttributes");


        // create modelVersion v2
        AtlasEntity existingModelVersionEntity = entityRetriever.toAtlasEntity(existingModelVersion.getGuid());
        AtlasVertex existingModelVersionVertex = entityRetriever.getEntityVertex(existingModelVersion.getGuid());
        AtlasVertex copyModelVertex = entityGraphMapper.createVertex(entityRetriever.toAtlasEntity(existingModelVersion.getGuid()));
        AtlasEntity copyModelVersion = entityRetriever.toAtlasEntity(copyModelVertex);
        setModelDates(copyModelVersion, copyModelVertex, now);
        String modelQualifiedName = (String) existingModelVersionEntity.getAttribute(QUALIFIED_NAME) + now;
        setQualifiedName(copyModelVersion, copyModelVertex, modelQualifiedName);
        setModelExpiredAtDates(existingModelVersionEntity, existingModelVersionVertex, now);

        // create entity e1'
        AtlasVertex copyEntityVertex = entityGraphMapper.createVertex(entity);
        AtlasEntity copyEntity = entityRetriever.toAtlasEntity(copyEntityVertex);
        setModelDates(copyEntity, copyEntityVertex, now);
        String entityQualifiedName = modelQualifiedName + "/" + entityName;
        setQualifiedName(copyEntity, copyEntityVertex, entityQualifiedName);
        setModelDates(copyEntity, copyEntityVertex, now);
        setModelExpiredAtDates(entity, vertex, now);

        // create modelVersion-modelEntity relationship
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship("d_m_version_d_m_entities");
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(copyModelVersion.getGuid(),
                copyModelVersion.getTypeName(), Collections.singletonMap(QUALIFIED_NAME, modelQualifiedName)));
        modelVersionEntityRelation.setEnd2(new AtlasObjectId(copyEntity.getGuid(),
                copyEntity.getTypeName(), Collections.singletonMap(QUALIFIED_NAME, entityQualifiedName)));
         atlasRelationshipStore.create(modelVersionEntityRelation);


       //  create modelEntity-modelAttributeRelationship
        List<AtlasEntity> entityAttributes = new ArrayList<>();
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship("d_m_entity_d_m_attributes");
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(copyEntity.getGuid(), copyEntity.getTypeName()));
        for (AtlasRelatedObjectId existingEntityAttribute: existingEntityAttributes){
            modelEntityAttributeRelation.setEnd2(
                    new AtlasObjectId(existingEntityAttribute.getGuid(), existingEntityAttribute.getTypeName()));
            atlasRelationshipStore.create(modelEntityAttributeRelation);
            entityAttributes.add(entityRetriever.toAtlasEntity(existingModelVersion.getGuid()));
        }

        // create model-modelVersion relation
        AtlasEntity.AtlasEntityWithExtInfo existingModelVersionExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionVertex, false);
        AtlasRelatedObjectId existingModel = (AtlasRelatedObjectId) existingModelVersionExtInfo.getEntity().getRelationshipAttributes().get("dMDataModel");
        AtlasRelationship modelVersionModelRelation = new AtlasRelationship("d_m_data_model_d_m_versions");
        modelVersionModelRelation.setEnd1(
                new AtlasObjectId(existingModel.getGuid(), existingModel.getTypeName()));
        modelVersionModelRelation.setEnd2(
                new AtlasObjectId(copyModelVersion.getGuid(), copyModelVersion.getTypeName()));
        atlasRelationshipStore.create(modelVersionModelRelation);


        /**
         * update context
         */
        // previousEntity and previousModelVersion
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasEntityType modelVersionType = typeRegistry.getEntityTypeByName(existingModelVersionEntity.getTypeName());

        context.addUpdated(entity.getGuid(), entity,
                entityType, vertex);

        context.addUpdated(existingModelVersionEntity.getGuid(), existingModelVersionEntity,
               modelVersionType, existingModelVersionVertex);

        // add createdEntity and createdModelVersion
        context.addCreated(copyEntity.getGuid(), copyEntity, entityType, copyEntityVertex);
        context.addCreated(copyModelVersion.getGuid(), copyModelVersion, modelVersionType, copyModelVertex);

        // resolve references
        context.getDiscoveryContext().
                addResolvedGuid(
                        existingModel.getGuid(),
                        entityRetriever.getEntityVertex(existingModel.getGuid()));

        // add attributes as relationship is updated : check
    }


    private void setModelDates(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(MODEL_SYSTEM_DATE, value);
        newEntity.setAttribute(MODEL_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_BUSINESS_DATE, value);
    }

    private void setModelExpiredAtDates(AtlasEntity oldEntity, AtlasVertex oldVertex, Object value) {
        oldEntity.setAttribute(MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        oldEntity.setAttribute(MODEL_EXPIRED_AT_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_BUSINESS_DATE, value);
    }

    private void setQualifiedName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(QUALIFIED_NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, QUALIFIED_NAME, value);
    }

    private boolean dmEntityExists(String dmEntityName) {
        return AtlasGraphUtilsV2.dmEntityExists(dmEntityName);
    }
}
