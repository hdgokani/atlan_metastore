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

    public void createInstanceConfig() throws AtlasBaseException {

        try {
            typeDefStore.getEntityDefByName("InstanceConfig");
        } catch (AtlasBaseException e) {
            AtlasTypesDef typesDef = new AtlasTypesDef();
            List<AtlasEntityDef> entityDefs = new ArrayList<>();
            AtlasEntityDef entityDef = createEntityDef();
            entityDefs.add(entityDef);
            typesDef.setEntityDefs(entityDefs);
            typeDefStore.createTypesDef(typesDef);
        } finally {
            addInstanceConfigEntity();
        }
    }

    private AtlasEntityDef createEntityDef() {
        AtlasEntityDef entityDef = new AtlasEntityDef();
        entityDef.addSuperType("Referenceable");
        entityDef.setName("InstanceConfig");
        entityDef.setServiceType("atlas_core");
        entityDef.setVersion(1L);
        AtlasStructDef.AtlasAttributeDef vertexIndexNameAttribute = createVertexIndexNameAttribute();
        entityDef.addAttribute(vertexIndexNameAttribute);
        return entityDef;
    }


    private AtlasStructDef.AtlasAttributeDef createVertexIndexNameAttribute() {
        AtlasStructDef.AtlasAttributeDef attributeDef = new AtlasStructDef.AtlasAttributeDef();
        attributeDef.setName("vertexIndexName");
        setCommonAttributeProperties(attributeDef);
        return attributeDef;
    }

    private void setCommonAttributeProperties(AtlasStructDef.AtlasAttributeDef idDef) {
        idDef.setTypeName("string");
        idDef.setIndexType(AtlasStructDef.AtlasAttributeDef.IndexType.STRING);
        idDef.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
        idDef.setIsIndexable(true);
        idDef.setIsOptional(false);
        idDef.setIsUnique(true);
        idDef.setSkipScrubbing(true);
        idDef.setIncludeInNotification(false);
    }


    public void addInstanceConfigEntity() throws AtlasBaseException {

        try {
            if (!getInstanceConfigEntity().isPresent()) {
                typeDefStore.getEntityDefByName("InstanceConfig");
                getInstanceConfigEntity();
                AtlasEntity instanceConfig = new AtlasEntity();
                instanceConfig.setTypeName("InstanceConfig");
                instanceConfig.setStatus(AtlasEntity.Status.ACTIVE);
                instanceConfig.setAttribute("qualifiedName", ELASTIC_INSTANCE_CONFIGURATION);
                instanceConfig.setAttribute("vertexIndexName", "vertex_index");
                instanceConfig.setVersion(0L);
                atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), false);
            }

        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    public String getCurrentIndexName() {
        Optional<AtlasEntityWithExtInfo> instanceConfig = getInstanceConfigEntity();
        if (instanceConfig.isPresent()) {
            return (String) instanceConfig.get().getEntity().getAttribute("vertexIndexName");
        } else {
            return "vertex_index";
        }
    }

    public String updateCurrentIndexName() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException(""));
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

    @Override
    public void stop() throws AtlasException {

    }
}
