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

    public static Map<String, Object>  getPreFilterDsl(String persona, String purpose, List<String> actions) {
        return ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
    }
}
