package org.apache.atlas.web.service;

import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.atlas.service.Service;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Order
public class ElasticInstanceConfigService implements Service {

    private static final String ELASTIC_INSTANCE_CONFIGURATION = "elastic_instance_configurations";

    private final AtlasTypeDefStore typeDefStore;
    private final AtlasEntityStore atlasEntityStore;
    private final AtlasTypeRegistry atlasTypeRegistry;

    public ElasticInstanceConfigService(AtlasTypeDefStore typeDefStore, AtlasEntityStore atlasEntityStore, AtlasTypeRegistry atlasTypeRegistry) {
        this.typeDefStore = typeDefStore;
        this.atlasEntityStore = atlasEntityStore;
        this.atlasTypeRegistry = atlasTypeRegistry;
    }

    @Override
    public void start() throws AtlasException {
        try {
            createInstanceConfig();
            String currentIndexName = getCurrentIndexName();
            ActiveIndexNameManager.setCurrentReadVertexIndexName(currentIndexName);
            ActiveIndexNameManager.setCurrentWriteVertexIndexName(currentIndexName);
        } catch (AtlasBaseException e) {
            throw new RuntimeException(e);
        }
    }

    public void createInstanceConfig() throws AtlasBaseException {
        try {
            typeDefStore.getEntityDefByName("InstanceConfig");
        } catch (AtlasBaseException e) {
            createTypeDef();
        } finally {
            createInstanceConfigEntity();
        }
    }

    private void createTypeDef() throws AtlasBaseException {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        List<AtlasEntityDef> entityDefs = new ArrayList<>();
        AtlasEntityDef entityDef = createEntityDef();
        entityDefs.add(entityDef);
        typesDef.setEntityDefs(entityDefs);
        typeDefStore.createTypesDef(typesDef);
    }

    private AtlasEntityDef createEntityDef() {
        AtlasEntityDef entityDef = new AtlasEntityDef();
        entityDef.setName("InstanceConfig");
        entityDef.setServiceType("atlas_core");
        entityDef.setVersion(1L);
        AtlasStructDef.AtlasAttributeDef vertexIndexNameAttribute = createVertexIndexNameAttribute();
        AtlasStructDef.AtlasAttributeDef qualifiedNameAttribute = createQualifiedNameAttribute();
        AtlasStructDef.AtlasAttributeDef isUpdateLockedAttribute = createIsUpdateLockedAttribute();
        entityDef.addAttribute(vertexIndexNameAttribute);
        entityDef.addAttribute(qualifiedNameAttribute);
        entityDef.addAttribute(isUpdateLockedAttribute);
        return entityDef;
    }

    private AtlasStructDef.AtlasAttributeDef createVertexIndexNameAttribute() {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("vertexIndexName");
        setCommonAttributeProperties(attributeDef);
        return attributeDef;
    }

    private AtlasStructDef.AtlasAttributeDef createQualifiedNameAttribute() {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("qualifiedName");
        setCommonAttributeProperties(attributeDef);
        return attributeDef;

    }

    private void setCommonAttributeProperties(AtlasStructDef.AtlasAttributeDef attributeDef) {
        attributeDef.setTypeName("string");
        attributeDef.setIndexType(AtlasStructDef.AtlasAttributeDef.IndexType.STRING);
        attributeDef.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        attributeDef.setIsIndexable(true);
        attributeDef.setIsOptional(false);
        attributeDef.setIsUnique(true);
        attributeDef.setSkipScrubbing(true);
        attributeDef.setIncludeInNotification(false);
    }

    private AtlasStructDef.AtlasAttributeDef createIsUpdateLockedAttribute() {

        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("isUpdateLocked");
        attributeDef.setTypeName("boolean");
        attributeDef.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        attributeDef.setIsIndexable(true);
        attributeDef.setIsOptional(false);
        attributeDef.setIsUnique(false);
        attributeDef.setSkipScrubbing(true);
        attributeDef.setIncludeInNotification(false);
        return attributeDef;
    }

    public void createInstanceConfigEntity() throws AtlasBaseException {
        if (!getInstanceConfigEntity().isPresent()) {
            typeDefStore.getEntityDefByName("InstanceConfig");
            AtlasEntity instanceConfig = new AtlasEntity();
            instanceConfig.setTypeName("InstanceConfig");
            instanceConfig.setStatus(AtlasEntity.Status.ACTIVE);
            instanceConfig.setAttribute("qualifiedName", ELASTIC_INSTANCE_CONFIGURATION);
            instanceConfig.setAttribute("vertexIndexName", "vertex_index");
            instanceConfig.setAttribute("isUpdateLocked", false);
            instanceConfig.setVersion(0L);
            atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), false);
        }
    }

    public String getCurrentIndexName() {
        return getInstanceConfigEntity()
                .map(config -> (String) config.getEntity().getAttribute("vertexIndexName"))
                .orElse("vertex_index");
    }

    public String updateCurrentIndexName() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException("The instance config doesn't exist"));
        String newIndexName = "vertex_index_" + System.currentTimeMillis();
        instanceConfig.getEntity().setAttribute("vertexIndexName", newIndexName);

        atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), true);

        return newIndexName;
    }

    private Optional<AtlasEntityWithExtInfo> getInstanceConfigEntity() {
        Map<String, Object> idAttribute = new HashMap<>();
        idAttribute.put("qualifiedName", ELASTIC_INSTANCE_CONFIGURATION);
        AtlasEntityType instanceConfigType;
        try {
            instanceConfigType = (AtlasEntityType) atlasTypeRegistry.getType("InstanceConfig");
            return Optional.of(atlasEntityStore.getByUniqueAttributes(instanceConfigType, idAttribute));
        } catch (AtlasBaseException e) {
            return Optional.empty();
        }
    }

    @Override
    public void stop() throws AtlasException {

    }
}
