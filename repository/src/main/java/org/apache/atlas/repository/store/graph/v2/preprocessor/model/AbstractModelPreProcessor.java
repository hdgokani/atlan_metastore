package org.apache.atlas.repository.store.graph.v2.preprocessor.model;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public abstract class AbstractModelPreProcessor implements PreProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractModelPreProcessor.class);
    private static final String ATTRIBUTE_TYPE = "DMAttribute";

    protected final AtlasTypeRegistry typeRegistry;

    protected final EntityGraphRetriever entityRetriever;

    protected EntityGraphMapper entityGraphMapper;
    protected AtlasRelationshipStore atlasRelationshipStore;

    public AbstractModelPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, EntityGraphMapper entityGraphMapper, AtlasRelationshipStore atlasRelationshipStore) {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityGraphMapper = entityGraphMapper;
        this.atlasRelationshipStore = atlasRelationshipStore;
    }

    protected static class ModelResponse {

        private AtlasEntity existingEntity;
        private AtlasEntity copyEntity;
        private AtlasVertex existingVertex;
        private AtlasVertex copyVertex;

        protected ModelResponse(AtlasEntity existingEntity, AtlasEntity copyEntity,
                                AtlasVertex existingVertex, AtlasVertex copyVertex) {
            this.existingEntity = existingEntity;
            this.copyEntity = copyEntity;
            this.existingVertex = existingVertex;
            this.copyVertex = copyVertex;
        }

        public AtlasEntity getExistingEntity() {
            return existingEntity;
        }

        public AtlasEntity getCopyEntity() {
            return copyEntity;
        }

        public AtlasVertex getExistingVertex() {
            return existingVertex;
        }

        public AtlasVertex getCopyVertex() {
            return copyVertex;
        }
    }

    protected void setModelDates(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(MODEL_SYSTEM_DATE, value);
        newEntity.setAttribute(MODEL_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, MODEL_BUSINESS_DATE, value);
    }

    protected void setModelExpiredAtDates(AtlasEntity oldEntity, AtlasVertex oldVertex, Object value) {
        oldEntity.setAttribute(MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        oldEntity.setAttribute(MODEL_EXPIRED_AT_BUSINESS_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_SYSTEM_DATE, value);
        AtlasGraphUtilsV2.setEncodedProperty(oldVertex, MODEL_EXPIRED_AT_BUSINESS_DATE, value);
    }

    protected void setQualifiedName(AtlasEntity newEntity, AtlasVertex newVertex, Object value) {
        newEntity.setAttribute(QUALIFIED_NAME, value);
        AtlasGraphUtilsV2.setEncodedProperty(newVertex, QUALIFIED_NAME, value);
    }

    protected ModelResponse replicateModelVersion(AtlasRelatedObjectId existingModelVersion, long epoch) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo existingModelVersionEntityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersion.getGuid());
        AtlasVertex existingModelVersionVertex = entityRetriever.getEntityVertex(existingModelVersion.getGuid());
        AtlasVertex copyModelVertex = entityGraphMapper.createVertex(existingModelVersionEntityWithExtInfo.getEntity());
        AtlasEntity copyModelVersion = entityRetriever.toAtlasEntity(copyModelVertex);
        copyAllAttributes(existingModelVersionEntityWithExtInfo.getEntity(), copyModelVersion,epoch);
        setModelDates(copyModelVersion, copyModelVertex, epoch);
        String modelQualifiedName = (String) existingModelVersionEntityWithExtInfo.getEntity().getAttribute(QUALIFIED_NAME) + epoch;
        setQualifiedName(copyModelVersion, copyModelVertex, modelQualifiedName);
        setModelExpiredAtDates(existingModelVersionEntityWithExtInfo.getEntity(), existingModelVersionVertex, epoch);
        return new ModelResponse(existingModelVersionEntityWithExtInfo.getEntity(), copyModelVersion, existingModelVersionVertex, copyModelVertex);
    }

    protected ModelResponse replicateModelEntity(AtlasEntity entity, AtlasVertex vertex, String modelQualifiedName, long epoch) throws AtlasBaseException {
        AtlasVertex copyEntityVertex = entityGraphMapper.createVertex(entity);
        AtlasEntity copyEntity = entityRetriever.toAtlasEntity(copyEntityVertex);
        copyAllAttributes(entity, copyEntity, epoch);
       // copyEntity.setRelationshipAttributes(entity.getRelationshipAttributes());
        setModelDates(copyEntity, copyEntityVertex, epoch);
        String entityQualifiedName = modelQualifiedName + "/" + entity.getAttribute(NAME);
        setQualifiedName(copyEntity, copyEntityVertex, entityQualifiedName);
        setModelDates(copyEntity, copyEntityVertex, epoch);
        setModelExpiredAtDates(entity, vertex, epoch);
        return new ModelResponse(entity, copyEntity, vertex, copyEntityVertex);
    }

//    protected ModelResponse replicateModelAttribute(){
//
//    }

    protected void  createModelVersionModelEntityRelationship(AtlasVertex modelVersionVertex, AtlasVertex modelEntityVertex) throws AtlasBaseException {
        AtlasRelationship modelVersionEntityRelation = new AtlasRelationship("d_m_version_d_m_entities");
        modelVersionEntityRelation.setEnd1(new AtlasObjectId(
                GraphHelper.getGuid(modelVersionVertex),
                GraphHelper.getTypeName(modelVersionVertex)));
        modelVersionEntityRelation.setEnd2(new AtlasObjectId(
               GraphHelper.getGuid(modelEntityVertex),
               GraphHelper.getTypeName(modelEntityVertex)));
     atlasRelationshipStore.create(modelVersionEntityRelation);
    }

    protected void createModelEntityModelAttributeRelation(AtlasVertex entity, List<AtlasRelatedObjectId> existingEntityAttributes) throws AtlasBaseException {
        AtlasRelationship modelEntityAttributeRelation = new AtlasRelationship("d_m_entity_d_m_attributes");
        modelEntityAttributeRelation.setEnd1(
                new AtlasObjectId(
                        GraphHelper.getGuid(entity),
                        GraphHelper.getTypeName(entity)));
        for (AtlasRelatedObjectId existingEntityAttribute : existingEntityAttributes) {
            modelEntityAttributeRelation.setEnd2(
                    new AtlasObjectId(
                            existingEntityAttribute.getGuid(),
                            existingEntityAttribute.getTypeName()));
            atlasRelationshipStore.create(modelEntityAttributeRelation);
        }
    }
    protected AtlasRelatedObjectId createModelModelVersionRelation(AtlasVertex existingModelVersionVertex, AtlasVertex latestModelVersionVertex) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo existingModelVersionExtInfo = entityRetriever.toAtlasEntityWithExtInfo(existingModelVersionVertex, false);
        AtlasRelatedObjectId existingModel = (AtlasRelatedObjectId) existingModelVersionExtInfo.getEntity().getRelationshipAttributes().get("dMDataModel");
        AtlasRelationship modelVersionModelRelation = new AtlasRelationship("d_m_data_model_d_m_versions");
        modelVersionModelRelation.setEnd1(
                new AtlasObjectId(
                        existingModel.getGuid(),
                       existingModel.getTypeName()));
        modelVersionModelRelation.setEnd2(
                new AtlasObjectId(
                        GraphHelper.getGuid(latestModelVersionVertex),
                        GraphHelper.getTypeName(latestModelVersionVertex)));
        atlasRelationshipStore.create(modelVersionModelRelation);
        return existingModel;
    }

    protected void copyAllAttributes(AtlasEntity source , AtlasEntity destination, long epochNow){
        destination.setAttributes(source.getAttributes());
        destination.setMeanings(source.getMeanings());
        destination.setCreateTime(new Date(epochNow));
        destination.setUpdateTime(new Date(epochNow));
    //    destination.setRelationshipAttributes(source.getRelationshipAttributes());
        destination.setCustomAttributes(source.getCustomAttributes());
        destination.setClassifications(source.getClassifications());
        destination.setAppendRelationshipAttributes(source.getAppendRelationshipAttributes());
        destination.setRemoveRelationshipAttributes(source.getRemoveRelationshipAttributes());
    }

    public static void replaceAttributes(Map<String, Object> existingAttributes, Map<String, Object> diffAttributes) {
       if (MapUtils.isEmpty(diffAttributes)){
           return;
       }
        // Temporary map to hold new key-value pairs during replacement
        Map<String, Object> tempMap = new HashMap<>();

        // Iterate over the original map
        for (Map.Entry<String, Object> entry : existingAttributes.entrySet()) {
            String originalKey = entry.getKey();
            Object value = entry.getValue();

            // Check if the second map contains a key for replacement
            if (diffAttributes.containsKey(originalKey)) {
                Object newValue = diffAttributes.get(originalKey);  // Get the new key from second map
                tempMap.put(originalKey, newValue);  // Put the new key in the temp map
            } else {
                tempMap.put(originalKey, value);  // No replacement, keep the original key
            }
        }

        // Clear the original map and put all the updated entries
        existingAttributes.clear();
        existingAttributes.putAll(tempMap);
    }
}
