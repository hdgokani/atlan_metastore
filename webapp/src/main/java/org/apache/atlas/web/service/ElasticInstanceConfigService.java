package org.apache.atlas.web.service;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.atlas.service.Service;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.TYPE_NAME_NOT_FOUND;
import static org.apache.atlas.service.ActiveIndexNameManager.DEFAULT_VERTEX_INDEX;

@Component
@Order(5)
@DependsOn(value = {"atlasTypeDefStoreInitializer", "atlasTypeDefGraphStoreV2"})
public class ElasticInstanceConfigService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticInstanceConfigService.class);

    private static final String ELASTIC_INSTANCE_CONFIGURATION = "elastic_instance_configurations";

    private static final String ELASTIC_INSTANCE_CONFIGURATION_TYPE_NAME = "__InstanceConfig";
    private static final String ELASTIC_INSTANCE_CONFIGURATION_ATTR_PREFIX = "esInstanceConfig.";

    private static final String ATTR_VERTEX_INDEX_NAME = ELASTIC_INSTANCE_CONFIGURATION_ATTR_PREFIX + "vertexIndexName";
    private static final String ATTR_UNIQUE_NAME       = ELASTIC_INSTANCE_CONFIGURATION_ATTR_PREFIX + "name";
    private static final String ATTR_IS_UPDATE_LOCKED  = ELASTIC_INSTANCE_CONFIGURATION_ATTR_PREFIX + "isUpdateLocked";

    private final AtlasEntityStore atlasEntityStore;
    private final AtlasTypeRegistry atlasTypeRegistry;

    @Inject
    public ElasticInstanceConfigService(AtlasEntityStore atlasEntityStore, AtlasTypeRegistry atlasTypeRegistry) {
        this.atlasEntityStore = atlasEntityStore;
        this.atlasTypeRegistry = atlasTypeRegistry;
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("==> ElasticInstanceConfigService.start()");
        try {
            ActiveIndexNameManager.init(getCurrentIndexName());
            createInstanceConfigEntity();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            LOG.info("<== ElasticInstanceConfigService.start()");
        }
    }

    public void createInstanceConfigEntity() throws AtlasBaseException {
        if (!getInstanceConfigEntity().isPresent()) {
            AtlasEntity instanceConfig = new AtlasEntity();
            instanceConfig.setTypeName(ELASTIC_INSTANCE_CONFIGURATION_TYPE_NAME);
            instanceConfig.setAttribute(ATTR_UNIQUE_NAME, ELASTIC_INSTANCE_CONFIGURATION);
            instanceConfig.setAttribute(ATTR_VERTEX_INDEX_NAME, DEFAULT_VERTEX_INDEX);
            instanceConfig.setAttribute(ATTR_IS_UPDATE_LOCKED, false);
            instanceConfig.setVersion(0L);
            atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), false);
        }
    }

    public String getCurrentIndexName() {
        return getInstanceConfigEntity()
                .map(config -> (String) config.getEntity().getAttribute(ATTR_VERTEX_INDEX_NAME))
                .orElse(DEFAULT_VERTEX_INDEX);
    }

    public String updateCurrentIndexName() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException("The instance config doesn't exist"));
        String newIndexName = ActiveIndexNameManager.getNewIndexName(RequestContext.get().getRequestTime());
        instanceConfig.getEntity().setAttribute(ATTR_VERTEX_INDEX_NAME, newIndexName);

        atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), true);

        return newIndexName;
    }

    public boolean isTypeDefUpdatesLocked() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException("The instance config doesn't exist"));
        return (boolean) instanceConfig.getEntity().getAttribute(ATTR_IS_UPDATE_LOCKED);
    }

    public boolean lockTypeDefUpdates() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException("The instance config doesn't exist"));
        instanceConfig.getEntity().setAttribute(ATTR_IS_UPDATE_LOCKED, true);
        atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), true);

        return true;
    }

    public boolean unlockTypeDefUpdates() throws AtlasBaseException {
        AtlasEntityWithExtInfo instanceConfig = getInstanceConfigEntity().orElseThrow(() -> new AtlasBaseException("The instance config doesn't exist"));
        instanceConfig.getEntity().setAttribute(ATTR_IS_UPDATE_LOCKED, true);
        atlasEntityStore.createOrUpdate(new AtlasEntityStream(instanceConfig), false);

        return true;
    }

    private Optional<AtlasEntityWithExtInfo> getInstanceConfigEntity() {
        Map<String, Object> idAttribute = new HashMap<>();
        idAttribute.put(ATTR_UNIQUE_NAME, ELASTIC_INSTANCE_CONFIGURATION);
        try {
            AtlasEntityType instanceConfigType = atlasTypeRegistry.getEntityTypeByName(ELASTIC_INSTANCE_CONFIGURATION_TYPE_NAME);
            if (instanceConfigType == null) {
                throw new AtlasBaseException(TYPE_NAME_NOT_FOUND, ELASTIC_INSTANCE_CONFIGURATION_TYPE_NAME);
            }

            return Optional.of(atlasEntityStore.getByUniqueAttributes(instanceConfigType, idAttribute));
        } catch (AtlasBaseException e) {
            return Optional.empty();
        }
    }

    @Override
    public void stop() throws AtlasException {

    }
}
