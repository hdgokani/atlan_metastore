package org.apache.atlas.authorizer;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.SKIP_DELETE_AUTH_CHECK_TYPES;
import static org.apache.atlas.repository.Constants.SKIP_UPDATE_AUTH_CHECK_TYPES;

public class AuthorizerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerUtils.class);

    private EntityDiscoveryService discoveryService;
    private static AtlasTypeRegistry typeRegistry;
    private static AuthorizerUtils  authorizerUtils;

    public AuthorizerUtils (EntityDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        try {
            this.discoveryService = discoveryService;
            this.typeRegistry = typeRegistry;

            LOG.info("==> AtlasAuthorization");
        } catch (Exception e) {
            LOG.error("==> AtlasAuthorization -> Error!");
        }
    }

    public static AuthorizerUtils getInstance(EntityDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        synchronized (AuthorizerUtils.class) {
            if (authorizerUtils == null) {
                authorizerUtils = new AuthorizerUtils(discoveryService, typeRegistry);
            }
            return authorizerUtils;
        }
    }

    public static void verifyUpdateEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_UPDATE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyEntityAccess(new AtlasEntity(entityHeader), AtlasPrivilege.ENTITY_UPDATE);
        }
    }

    public static void verifyDeleteEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_DELETE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyEntityAccess(new AtlasEntity(entityHeader), AtlasPrivilege.ENTITY_DELETE);
        }
    }

    public static void verifyEntityAccess(AtlasEntity entity, AtlasPrivilege action) throws AtlasBaseException {
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            //if (AtlasPrivilege.ENTITY_CREATE == action) {
            if (!EntityAuthorizer.isAccessAllowed(entity, action.getType())){
                String message = action.getType() + ":" + entity.getTypeName() + ":" + entity.getAttributes().get(QUALIFIED_NAME);
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, userName, message);
            }
            //}
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    public static void verifyRelationshipAccess(String action, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (!RelationshipAuthorizer.isAccessAllowed(action, endOneEntity, endTwoEntity)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(),
                        action + ":" + endOneEntity.getTypeName() + "|" + endTwoEntity.getTypeName());
            }
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    public static Map<String, Object>  getPreFilterDsl(String persona, String purpose, List<String> actions) {
        return ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
    }
}
