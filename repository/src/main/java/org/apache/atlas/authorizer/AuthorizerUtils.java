package org.apache.atlas.authorizer;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;

import static org.apache.atlas.authorize.AtlasPrivilege.ENTITY_CREATE;
import static org.apache.atlas.repository.Constants.SKIP_DELETE_AUTH_CHECK_TYPES;
import static org.apache.atlas.repository.Constants.SKIP_UPDATE_AUTH_CHECK_TYPES;

@Component
public class AuthorizerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerUtils.class);

    private static AtlasTypeRegistry typeRegistry;

    private static boolean useAbacAuthorizer = false;

    static {
        try {
            useAbacAuthorizer = ApplicationProperties.get().getBoolean("atlas.authorizer.enable.abac", false);

            if (useAbacAuthorizer) {
                LOG.info("Using abac authorizer");
            }
        } catch (AtlasException e) {
            LOG.warn("Failed to read conf `atlas.authorizer.impl`, falling back to use Atlas authorizer instead of abac");
        }
    }

    @Inject
    public AuthorizerUtils(AtlasGraph graph, AtlasTypeRegistry typeRegistry) throws IOException {
        this.typeRegistry = typeRegistry;
    }

    public static boolean isUseAbacAuthorizer() {
        return useAbacAuthorizer;
    }

    public static void verifyUpdateEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_UPDATE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyAccess(entityHeader, AtlasPrivilege.ENTITY_UPDATE);
        }
    }

    public static void verifyDeleteEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_DELETE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyAccess(entityHeader, AtlasPrivilege.ENTITY_DELETE);
        }
    }

    public static void verifyAccess(AtlasEntityHeader entityHeader, AtlasPrivilege action) throws AtlasBaseException {
        if (!useAbacAuthorizer) {
            AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder(typeRegistry, action, entityHeader);
            AtlasEntityAccessRequest entityAccessRequest = requestBuilder.build();
            AtlasAuthorizationUtils.verifyAccess(entityAccessRequest, action + "guid=" + entityHeader.getGuid());

        } else {
            if (action == ENTITY_CREATE) {
                NewAuthorizerUtils.verifyEntityCreateAccess(new AtlasEntity(entityHeader), ENTITY_CREATE);
            } else {
                NewAuthorizerUtils.verifyAccess(entityHeader, action);
            }
        }
    }

    public static void verifyAccessForEvaluator(AtlasEntityHeader entityHeader, AtlasPrivilege action) throws AtlasBaseException {
        if (!useAbacAuthorizer) {
            AtlasClassification classification = CollectionUtils.isNotEmpty(entityHeader.getClassifications()) ? entityHeader.getClassifications().get(0) : null;
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, action, entityHeader, classification));
        } else {
            if (action == ENTITY_CREATE) {
                AuthorizerUtils.verifyAccess(entityHeader, ENTITY_CREATE);
            } else {
                if (StringUtils.isNotEmpty(entityHeader.getGuid())) {
                    NewAuthorizerUtils.verifyAccess(entityHeader, action);
                } else {
                    NewAuthorizerUtils.verifyAccessForEvaluator(entityHeader, action);
                }
            }
        }
    }

    public static void verifyRelationshipAccess(AtlasPrivilege action, String relationShipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        if (!useAbacAuthorizer) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasRelationshipAccessRequest(typeRegistry, action, relationShipType, endOneEntity, endTwoEntity));
        } else {
            NewAuthorizerUtils.verifyRelationshipAccessInMem(action,
                    relationShipType,
                    endOneEntity,
                    endTwoEntity);
            /*if (action == AtlasPrivilege.RELATIONSHIP_ADD) {
                NewAuthorizerUtils.verifyRelationshipCreateAccess(AtlasPrivilege.RELATIONSHIP_ADD,
                        relationShipType,
                        endOneEntity,
                        endTwoEntity);
            } else {
                NewAuthorizerUtils.verifyRelationshipAccess(action,
                        relationShipType,
                        endOneEntity,
                        endTwoEntity);
            }*/
        }
    }
}
