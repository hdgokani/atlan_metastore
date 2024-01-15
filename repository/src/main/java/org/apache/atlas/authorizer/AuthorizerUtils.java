package org.apache.atlas.authorizer;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.audit.provider.AuditHandler;
import org.apache.atlas.authorize.AtlasAccessorRequest;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommon;
import org.apache.atlas.authorizer.authorizers.EntityAuthorizer;
import org.apache.atlas.authorizer.authorizers.ListAuthorizer;
import org.apache.atlas.authorizer.authorizers.RelationshipAuthorizer;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerServiceDef;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.SKIP_DELETE_AUTH_CHECK_TYPES;
import static org.apache.atlas.repository.Constants.SKIP_UPDATE_AUTH_CHECK_TYPES;

@Component
public class AuthorizerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthorizerUtils.class);

    public static final String POLICY_TYPE_ALLOW = "allow";
    public static final String POLICY_TYPE_DENY = "deny";

    private static AtlasTypeRegistry typeRegistry;
    private static EntityGraphRetriever entityRetriever;
    private static AuditHandler auditProvider;


    private static RangerServiceDef SERVICE_DEF_ATLAS = null;
    //private static RangerServiceDef SERVICE_DEF_ATLAS_TAG = null;


    @Inject
    public AuthorizerUtils(AtlasGraph graph, AtlasTypeRegistry typeRegistry) throws IOException {
        this.typeRegistry = typeRegistry;
        this.entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);

        //String path = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY) + File.separator +  "atlas-atlas-audit.xml";
        //FileInputStream fis = new FileInputStream(path);

       /* Properties properties = new Properties();
        //properties.load(fis);
        properties.put(AUDIT_IS_ENABLED_PROP, true);
        properties.put(AUDIT_LOG4J_IS_ENABLED_PROP, true);
        properties.put("xasecure.audit.destination.log4j", true);
        properties.put("xasecure.audit.destination.log4j.logger", true);
*/


        //AsyncAuditProvider asyncAuditProvider = new AsyncAuditProvider();

        SERVICE_DEF_ATLAS = getResourceAsObject("/service-defs/atlas-servicedef-atlas.json", RangerServiceDef.class);
        //SERVICE_DEF_ATLAS_TAG = getResourceAsObject("/service-defs/atlas-servicedef-atlas_tag.json", RangerServiceDef.class);
    }

    public  <T> T getResourceAsObject(String resourceName, Class<T> clazz) throws IOException {
        InputStream stream = getClass().getResourceAsStream(resourceName);
        return AtlasType.fromJson(stream, clazz);
    }

    public static void verifyUpdateEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_UPDATE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyAccess(entityHeader.getGuid(), AtlasPrivilege.ENTITY_UPDATE);
        }
    }

    public static void verifyDeleteEntityAccess(AtlasEntityHeader entityHeader) throws AtlasBaseException {
        if (!SKIP_DELETE_AUTH_CHECK_TYPES.contains(entityHeader.getTypeName())) {
            verifyAccess(entityHeader.getGuid(), AtlasPrivilege.ENTITY_DELETE);
        }
    }

    public static void verifyEntityCreateAccess(AtlasEntity entity, AtlasPrivilege action) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyEntityCreateAccess");
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (AtlasPrivilege.ENTITY_CREATE == action) {
                if (!EntityAuthorizer.isAccessAllowedInMemory(entity, action.getType())){
                    String message = action.getType() + ":" + entity.getTypeName() + ":" + entity.getAttributes().get(QUALIFIED_NAME);
                    throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, userName, message);
                }
            }
        } catch (AtlasBaseException e) {
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static void verifyAccess(String guid, AtlasPrivilege action) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyAccess");
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(guid);

        AtlasEntityAccessRequest request = new AtlasEntityAccessRequest(typeRegistry, action, entity);

        NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);



        try {
            AccessResult result = EntityAuthorizer.isAccessAllowed(guid, action.getType());
            result.setAtlasAccessRequest(request);

            if (!result.isAllowed()) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, userName, action + ":" + guid);
            }
            auditHandler.processResult(result);

        } catch (AtlasBaseException e) {
            throw e;
        } finally {
            auditHandler.flushAudit();
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static void verifyAccessForEvaluator(String entityTypeName, String entityQualifiedName, String action) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyAccess");
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (!EntityAuthorizer.isAccessAllowedEvaluator(entityTypeName, entityQualifiedName, action)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, userName, action + ":" + entityTypeName + ":" + entityQualifiedName);
            }
        } catch (AtlasBaseException e) {
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static void verifyRelationshipAccess(AtlasPrivilege action, String relationShipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyAccess");
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }


        AtlasRelationshipAccessRequest request = new AtlasRelationshipAccessRequest(typeRegistry,
                action,
                relationShipType,
                endOneEntity,
                endTwoEntity);

        NewAtlasAuditHandler auditHandler = new NewAtlasAuditHandler(request, SERVICE_DEF_ATLAS);

        try {
            AccessResult result = RelationshipAuthorizer.isRelationshipAccessAllowed(action.getType(), endOneEntity, endTwoEntity);
            result.setAtlasAccessRequest(request);

            if (!result.isAllowed()) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), action + "|" + endOneEntity.getGuid() + "|" + endTwoEntity.getGuid());
            }
            auditHandler.processResult(result);
        } catch (AtlasBaseException e) {
            throw e;
        } finally {
            auditHandler.flushAudit();
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static void verifyRelationshipCreateAccess(String action, String relationshipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("verifyAccess");
        String userName = AuthorizerCommon.getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (!RelationshipAuthorizer.isAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(),
                        action + ":" + endOneEntity.getTypeName() + "|" + endTwoEntity.getTypeName());
            }
        } catch (AtlasBaseException e) {
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static AtlasAccessorResponse getAccessors(AtlasAccessorRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("AuthorizerUtils.getAccessors");

        try {
            return AccessorsExtractor.getAccessors(request);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static Map<String, Object>  getPreFilterDsl(String persona, String purpose, List<String> actions) {
        return ListAuthorizer.getElasticsearchDSL(persona, purpose, actions);
    }
}
