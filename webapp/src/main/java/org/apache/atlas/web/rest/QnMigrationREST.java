package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.*;

@Path("migration")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class QnMigrationREST {

    private static final Logger LOG = LoggerFactory.getLogger(QnMigrationREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.QnMigration");

    private final AtlasEntityStore entityStore;
    private final EntityDiscoveryService discovery;
    private final EntityGraphRetriever entityRetriever;
    List<AtlasEntityHeader> policies;
    Map<String, String> updatedPolicyResources;

    @Inject
    public QnMigrationREST(AtlasEntityStore entityStore, EntityDiscoveryService discovery, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;
        this.discovery = discovery;
    }

    @POST
    @Path("updateQn")
    @Timed
    public EntityMutationResponse updateQn () throws Exception {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response = new EntityMutationResponse();
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.updateQn()");
            }
            Set<String> attributes = new HashSet<>(Arrays.asList(QUALIFIED_NAME, SUPER_DOMAIN_QN, PARENT_DOMAIN_QN, "__customAttributes"));
            List<AtlasEntityHeader> entities = getEntity(DATA_DOMAIN_ENTITY_TYPE, attributes, null);

            // Process entities in chunks of 20
            for (int i = 0; i < entities.size(); i += 20) {
                List<AtlasEntityHeader> chunk = entities.subList(i, Math.min(i + 20, entities.size()));
                updateChunk(chunk);
            }

            return response;
        } catch (Exception e) {
            LOG.error("Error while updating qualified names", e);
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }

    }

    private void updateChunk (List<AtlasEntityHeader> chunk) throws AtlasBaseException {
        for (AtlasEntityHeader entity : chunk) {
            AtlasEntity atlasEntity = entityStore.getById(entity.getGuid()).getEntity();
            AtlasVertex vertex = entityRetriever.getEntityVertex(entity.getGuid());
            String qualifiedName = (String) atlasEntity.getAttribute(QUALIFIED_NAME);
            String parentDomainQualifiedName = (String) atlasEntity.getAttribute(PARENT_DOMAIN_QN);
            String superDomainQualifiedName = (String) atlasEntity.getAttribute(SUPER_DOMAIN_QN);

            Map<String,String> customAttributes = atlasEntity.getCustomAttributes();
            if(customAttributes != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE) != null && customAttributes.get(MIGRATION_CUSTOM_ATTRIBUTE).equals("true")){
                LOG.info("Entity already migrated for entity: {}", qualifiedName);
            }
            else {
                migrateDomainAttributes(atlasEntity, vertex, parentDomainQualifiedName, superDomainQualifiedName);
                updatePolicy(this.policies,this.updatedPolicyResources);
                LOG.info("Migrated qualified name for entity: {}", qualifiedName);
            }
        }
    }

    @GraphTransaction
    private void migrateDomainAttributes(AtlasEntity entity, AtlasVertex vertex, String parentDomainQualifiedName, String rootDomainQualifiedName) throws AtlasBaseException {
        LOG.info("Migrating qualified name for entity: {}", entity.getAttribute(QUALIFIED_NAME));

        String currentQualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
        String updatedQualifiedName = createDomainQualifiedName(parentDomainQualifiedName);
        vertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);

        if (StringUtils.isEmpty(parentDomainQualifiedName) && StringUtils.isEmpty(rootDomainQualifiedName)){
            rootDomainQualifiedName = updatedQualifiedName;
        }
        else{
            vertex.setProperty(PARENT_DOMAIN_QN, parentDomainQualifiedName);
            vertex.setProperty(SUPER_DOMAIN_QN, rootDomainQualifiedName);
        }

        String currentResource = "entity:"+ currentQualifiedName;
        String updatedResource = "entity:"+ updatedQualifiedName;

        this.updatedPolicyResources.put(currentResource, updatedResource);

        this.policies = getEntity(POLICY_ENTITY_TYPE, new HashSet<>(Arrays.asList(ATTR_POLICY_RESOURCES, ATTR_POLICY_CATEGORY)), currentQualifiedName);

        Map<String,String> customAttributes = new HashMap<>();
        customAttributes.put(MIGRATION_CUSTOM_ATTRIBUTE, "true");
        vertex.setProperty(CUSTOM_ATTRIBUTES_PROPERTY_KEY, customAttributes);

        Iterator<AtlasVertex> products = getActiveChildrenVertices(vertex, DATA_PRODUCT_EDGE_LABEL);

        while (products.hasNext()) {
            AtlasVertex productVertex = products.next();
            AtlasEntity productEntity = entityRetriever.toAtlasEntity(productVertex);
            migrateDataProductAttributes(productEntity, productVertex, updatedQualifiedName, rootDomainQualifiedName);
        }

        // Get all children domains of current domain
        Iterator<AtlasVertex> childDomains = getActiveChildrenVertices(vertex, DOMAIN_PARENT_EDGE_LABEL);

        while (childDomains.hasNext()) {
            AtlasVertex childVertex = childDomains.next();
            AtlasEntity childEntity = entityRetriever.toAtlasEntity(childVertex);
            migrateDomainAttributes(childEntity, childVertex, updatedQualifiedName, rootDomainQualifiedName);
        }

    }

    private void migrateDataProductAttributes(AtlasEntity entity, AtlasVertex vertex, String parentDomainQualifiedName, String rootDomainQualifiedName) throws AtlasBaseException {
        String updatedQualifiedName = createProductQualifiedName(parentDomainQualifiedName);
        vertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);

        vertex.setProperty(PARENT_DOMAIN_QN, parentDomainQualifiedName);
        vertex.setProperty(SUPER_DOMAIN_QN, rootDomainQualifiedName);
    }

    protected void updatePolicy(List<AtlasEntityHeader> policies, Map<String, String> updatedPolicyResources) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.updateQn()");
            }

            LOG.info("Updating policy for entities {}", updatedPolicyResources);

            if (CollectionUtils.isNotEmpty(policies)) {
                for (AtlasEntityHeader policy : policies) {
                    AtlasVertex policyVertex = entityRetriever.getEntityVertex(policy.getGuid());

                    AtlasEntity policyEntity = entityRetriever.toAtlasEntity(policyVertex);

                    List<String> policyResources = (List<String>) policyEntity.getAttribute(ATTR_POLICY_RESOURCES);
                    List<String> updatedPolicyResourcesList = new ArrayList<>();

                    for (String resource : policyResources) {
                        if (updatedPolicyResources.containsKey(resource)) {
                            updatedPolicyResourcesList.add(updatedPolicyResources.get(resource));
                        } else {
                            updatedPolicyResourcesList.add(resource);
                        }
                    }

                    policyVertex.removeProperty(ATTR_POLICY_RESOURCES);
                    policyVertex.setProperty(ATTR_POLICY_RESOURCES, updatedPolicyResourcesList);

                    EntityStream policyEntityStream = new AtlasEntityStream(policyEntity);
                    EntityMutationResponse response = entityStore.createOrUpdate(policyEntityStream, false);

                }
            }

        }finally {
            AtlasPerfTracer.log(perf);
        }

    }


    private static String createDomainQualifiedName(String parentDomainQualifiedName) {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName)) {
            return parentDomainQualifiedName + "/domain/" + getUUID();
        } else{
            return "default/domain" + "/" + getUUID();
        }
    }

    private static String createProductQualifiedName(String parentDomainQualifiedName) throws AtlasBaseException {
        if (StringUtils.isEmpty(parentDomainQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Parent Domain Qualified Name cannot be empty or null");
        }
        return parentDomainQualifiedName + "/product/" + getUUID();
    }

    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    protected List<AtlasEntityHeader> getEntity(String entityType, Set<String> attributes, String resource) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MigrationREST.searchUsingDslQuery()");
            }


            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", entityType)));

            if(entityType.equals(DATA_DOMAIN_ENTITY_TYPE)){
                Map<String, Object> childBool = new HashMap<>();
                List <Map<String, Object>> mustNotClauseList = new ArrayList<>();
                mustNotClauseList.add(mapOf("exists", mapOf("field", PARENT_DOMAIN_QN)));

                Map<String, Object> shouldBool = new HashMap<>();
                shouldBool.put("must_not", mustNotClauseList);

                List <Map<String, Object>> shouldClauseList = new ArrayList<>();
                shouldClauseList.add(mapOf("bool", shouldBool));

                childBool.put("should", shouldClauseList);
                mustClauseList.add(mapOf("bool", childBool));
            }

            if(entityType.equals(POLICY_ENTITY_TYPE)){
                mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
                mustClauseList.add(mapOf("terms", mapOf("policyResources", Arrays.asList(resource))));
            }

            Map<String, Object> bool = new HashMap<>();
            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));


            List<AtlasEntityHeader> entities = indexSearchPaginated(dsl, attributes, discovery);

            return entities;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    public static List<AtlasEntityHeader> indexSearchPaginated(Map<String, Object> dsl, Set<String> attributes, EntityDiscoveryService discovery) throws AtlasBaseException {
        IndexSearchParams searchParams = new IndexSearchParams();
        List<AtlasEntityHeader> ret = new ArrayList<>();

        List<Map> sortList = new ArrayList<>(0);
        sortList.add(mapOf("__timestamp", mapOf("order", "asc")));
        sortList.add(mapOf("__guid", mapOf("order", "asc")));
        dsl.put("sort", sortList);

        int from = 0;
        int size = 100;
        boolean hasMore = true;
        do {
            dsl.put("from", from);
            dsl.put("size", size);
            searchParams.setDsl(dsl);

            if (CollectionUtils.isNotEmpty(attributes)) {
                searchParams.setAttributes(attributes);
            }

            List<AtlasEntityHeader> headers = discovery.directIndexSearch(searchParams).getEntities();

            if (CollectionUtils.isNotEmpty(headers)) {
                ret.addAll(headers);
            } else {
                hasMore = false;
            }

            from += size;

        } while (hasMore);

        return ret;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
