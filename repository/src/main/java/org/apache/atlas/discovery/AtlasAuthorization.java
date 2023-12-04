package org.apache.atlas.discovery;

import com.datastax.oss.driver.internal.core.util.CollectionsUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
//import org.apache.atlas.model.audit.AuditSearchParams;
//import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.model.RangerRole;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

import java.util.*;
import java.util.stream.Collectors;

import org.elasticsearch.client.RestClient;
import org.janusgraph.graphdb.util.CollectionsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class AtlasAuthorization {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorization.class);

    public static final String PERSONA_ENTITY_TYPE   = "Persona";
    private String policiesString;
    private ObjectMapper mapper = new ObjectMapper();
    private IndexAliasStore aliasStore;
    private Map<String, Set> userPoliciesMap= new HashMap<>();

    private EntityDiscoveryService discoveryService;
//    private ESBasedAuditRepository auditRepository;

    private static AtlasAuthorization  atlasAuthorization;
    private static UsersGroupsRolesStore  usersGroupsRolesStore;
    private List<AtlasEntityHeader> allPolicies;
    private long lastUpdatedTime = -1;
    private List<String> serviceNames = new ArrayList<>();
    private static Map<String, String> esEntityAttributeMap = new HashMap<>();




    public static AtlasAuthorization getInstance(EntityDiscoveryService discoveryService) {
        synchronized (AtlasAuthorization.class) {
            if (atlasAuthorization == null) {
                atlasAuthorization = new AtlasAuthorization(discoveryService);
            }
            return atlasAuthorization;
        }
    }

    public static AtlasAuthorization getInstance() {
        if (atlasAuthorization != null) {
            return atlasAuthorization;
        }
        return null;
    }

    public AtlasAuthorization (EntityDiscoveryService discoveryService) {
        try {
            this.discoveryService = discoveryService;
//            auditRepository = null;

            this.usersGroupsRolesStore = UsersGroupsRolesStore.getInstance();

            serviceNames.add("atlas");
            serviceNames.add("atlas_tag");
            serviceNames.add("ape");

            esEntityAttributeMap.put("__typeName.keyword", "__typeName");

//            PolicyRefresher policyRefresher = new PolicyRefresher();
//            policyRefresher.start();

            LOG.info("==> AtlasAuthorization");
        } catch (Exception e) {
            LOG.error("==> AtlasAuthorization -> Error!");
        }
    }

    private static Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static void verifyAccess(String guid, String action) throws AtlasBaseException {
        try {
            if (!isAccessAllowed(guid, action)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), guid);
            }
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    public static void verifyAccess(AtlasEntity entity, AtlasPrivilege action, String message) throws AtlasBaseException {
        try {
            if (AtlasPrivilege.ENTITY_CREATE == action) {
                if (!isCreateAccessAllowed(entity, AtlasPrivilege.ENTITY_CREATE.getType())){
                    throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, message);
                }
            }
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    private static boolean isAccessAllowed(String guid, String action) throws AtlasBaseException {
        if (guid == null) {
            return false;
        }
        List<Map<String, Object>> filterClauseList = new ArrayList<>();
        Map<String, Object> policiesDSL = getElasticsearchDSL(null, null, Arrays.asList(action));
        filterClauseList.add(policiesDSL);
        filterClauseList.add(getMap("term", getMap("__guid", guid)));
        Map<String, Object> dsl = getMap("query", getMap("bool", getMap("filter", filterClauseList)));
        ObjectMapper mapper = new ObjectMapper();
        String dslString = null;
        Integer count = null;
        try {
            dslString = mapper.writeValueAsString(dsl);
            count = getCountFromElasticsearch(dslString);
            LOG.info(dslString);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        if (count != null && count > 0) {
            return true;
        }
        return false;
    }

    public static void verifyAccess(String entityTypeName, String entityQualifiedName, String action) throws AtlasBaseException {
        try {
            if (!isAccessAllowed(entityTypeName, entityQualifiedName, action)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Unauthorised");
            }
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    private static boolean isAccessAllowed(String entityTypeName, String entityQualifiedName, String action) throws AtlasBaseException {
        List<Map<String, Object>> filterClauseList = new ArrayList<>();
        Map<String, Object> policiesDSL = getElasticsearchDSL(null, null, Arrays.asList(action));
        filterClauseList.add(policiesDSL);
        filterClauseList.add(getMap("term", getMap("__typeName.keyword", entityTypeName)));
        if (entityQualifiedName != null)
            filterClauseList.add(getMap("term", getMap("qualifiedName", entityQualifiedName)));
        Map<String, Object> dsl = getMap("query", getMap("bool", getMap("filter", filterClauseList)));
        ObjectMapper mapper = new ObjectMapper();
        String dslString = null;
        Integer count = null;
        try {
            dslString = mapper.writeValueAsString(dsl);
            count = getCountFromElasticsearch(dslString);
            LOG.info(dslString);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        if (count != null && count > 0) {
            return true;
        }
        return false;
    }

    public static boolean isRelationshipAccessAllowed(String endOneGuid, String endTwoGuid, String action) throws AtlasBaseException {
        if (endOneGuid == null || endTwoGuid == null) {
            return false;
        }
        List<Map<String, Object>> clauses = null;
        try {
            clauses = getElasticsearchDSLForRelationshipActions(Arrays.asList(action), endOneGuid, endTwoGuid);
        } catch (JsonProcessingException e) {
            return false;
        }
        Map<String, Object> dsl = getMap("query", getMap("bool", getMap("should", clauses)));
        try {
            ObjectMapper mapper = new ObjectMapper();
            String dslString = mapper.writeValueAsString(dsl);
            Integer count = getCountFromElasticsearch(dslString);
            if (count != null && count == 2) {
                return true;
            }
            LOG.info(dslString);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return false;
    }

//    public static void verifyAccess(AtlasEntity entity, String action) throws AtlasBaseException {
//        try {
//            if (!isCreateAccessAllowed(entity, action)) {
//                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Unauthorised");
//            }
//        } catch (AtlasBaseException e) {
//            throw e;
//        }
//    }

    public static boolean isCreateAccessAllowed(AtlasEntity entity, String action) {
        List<RangerPolicy> policies = getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action));
        List<String> filterCriteriaList = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            String filterCriteria = policy.getPolicyFilterCriteria();
            if (filterCriteria != null && !filterCriteria.isEmpty() ) {
                filterCriteriaList.add(filterCriteria);
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        boolean ret = false;
        boolean eval;
        for (String filterCriteria: filterCriteriaList) {
            eval = false;
            JsonNode filterCriteriaNode = null;
            try {
                filterCriteriaNode = mapper.readTree(filterCriteria);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, entity);
            }
            ret = ret || eval;
            if (ret) {
                break;
            }
        }

        if (!ret) {
            List<RangerPolicy> tagPolicies = getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action));
            List<RangerPolicy> resourcePolicies = getRelevantPolicies(null, null, "atlas", Collections.singletonList(action));

            tagPolicies.addAll(resourcePolicies);

            ret = validateResourcesForCreateEntity(tagPolicies, entity);
        }

        return ret;
    }

    private static boolean validateResourcesForCreateEntity(List<RangerPolicy> resourcePolicies, AtlasEntity entity) {

        for (RangerPolicy rangerPolicy : resourcePolicies) {
            Map<String, RangerPolicy.RangerPolicyResource> resources = rangerPolicy.getResources();

            boolean allStar = true;

            for (String resource : resources.keySet()) {
                if (!resources.get(resource).getValues().contains("*")){
                    allStar = false;
                    break;
                }
            }

            if (allStar) {
                return true;

            } else {
                boolean resourcesMatched = true;

                for (String resource : resources.keySet()) {
                    List<String> values = resources.get(resource).getValues();

                    if ("entity-type".equals(resource)) {
                        String assetTypeName = entity.getTypeName();
                        Optional<String> match = values.stream().filter(x -> assetTypeName.matches(x.replace("*", ".*"))).findFirst();

                        if (!match.isPresent()) {
                            resourcesMatched = false;
                            break;
                        }
                    }

                    if ("entity".equals(resource)) {
                        String assetQualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
                        Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x.replace("*", ".*"))).findFirst();

                        if (!match.isPresent()) {
                            resourcesMatched = false;
                            break;
                        }
                    }

                    //for tag based policy
                    if ("tag".equals(resource)) {
                        List<String> assetTags = entity.getClassifications().stream().map(x -> x.getTypeName()).collect(Collectors.toList());

                        for (String assetTag : assetTags) {
                            Optional<String> match = values.stream().filter(x -> assetTag.matches(x.replace("*", ".*"))).findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }
                }

                if (resourcesMatched) {
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean validateFilterCriteriaWithEntity(JsonNode data, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder convertJsonToQueryMetrics = RequestContext.get().startMetricRecord("convertJsonToQuery");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        boolean result = true;
        boolean evaluation;

        if (criterion.size() == 0) {
            return false;
        }

        for (JsonNode crit : criterion) {

            evaluation = false;

            if (crit.has("condition")) {
                evaluation = validateFilterCriteriaWithEntity(crit, entity);

            } else {

                String operator = crit.get("operator").asText();
                String attributeName = crit.get("attributeName").asText();
                String attributeValue = crit.get("attributeValue").asText();

                if (esEntityAttributeMap.get(attributeName) != null) {
                    attributeName = esEntityAttributeMap.get(attributeName);
                }

                String entityAttributeValue = (String) entity.getAttribute(attributeName);
                if (operator.equals("EQUALS") && attributeValue.equals(entityAttributeValue)) {
                    evaluation = true;
                } else if ((operator.equals("STARTS_WITH") && entityAttributeValue.startsWith(attributeValue))) {
                    evaluation = true;
                } else if ((operator.equals("ENDS_WITH") && entityAttributeValue.endsWith(attributeValue))) {
                    evaluation = true;
                } else if ((operator.equals("NOT_EQUALS") && !entityAttributeValue.equals(attributeValue))) {
                    evaluation = true;
                }
            }

            if (condition.equals("AND")) {
                result = result && evaluation;
            } else {
                result = result || evaluation;
            }
        }

        RequestContext.get().endMetricRecord(convertJsonToQueryMetrics);
        return result;
    }

    private static Integer getCountFromElasticsearch(String query) throws AtlasBaseException {
        RestClient restClient = getLowLevelClient();
        AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
        Map<String, Object> elasticsearchResult = null;
        elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(query);
        Integer count = null;
        if (elasticsearchResult!=null) {
            count = (Integer) elasticsearchResult.get("total");
        }
        return count;
    }

    public static List<Map<String, Object>> getElasticsearchDSLForRelationshipActions(List<String> actions, String endOneGuid, String endTwoGuid) throws JsonProcessingException {
        List<Map<String, Object>> clauses = new ArrayList<>();
        List<RangerPolicy> resourcePolicies = getRelevantPolicies(null, null, "atlas", actions);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForRelationshipResourcePolicies(resourcePolicies, endOneGuid, endTwoGuid);

        List<RangerPolicy> tagPolicies = getRelevantPolicies(null, null, "atlas_tag", actions);
        Map<String, Object> tagPoliciesClause = getDSLForTagResourcePolicies(tagPolicies, endOneGuid, endTwoGuid);

        List<RangerPolicy> abacPolicies = getRelevantPolicies(null, null, "atlas_abac", actions);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForRelationshipAbacPolicies(abacPolicies, endOneGuid, endTwoGuid);

        clauses.addAll(resourcePoliciesClauses);
        if (tagPoliciesClause != null) {
            clauses.add(tagPoliciesClause);
        }
        clauses.addAll(abacPoliciesClauses);

        return clauses;
    }

    public static Map<String, Object> getElasticsearchDSL(String persona, String purpose, List<String> actions) {

        List<RangerPolicy> resourcePolicies = getRelevantPolicies(persona, purpose, "atlas", actions);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = getRelevantPolicies(persona, purpose, "atlas_tag", actions);
        Map<String, Object> tagPoliciesClause = getDSLForTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = getRelevantPolicies(persona, purpose, "atlas_abac", actions);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForAbacPolicies(abacPolicies);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.addAll(resourcePoliciesClauses);
        if (tagPoliciesClause != null) {
            shouldClauses.add(tagPoliciesClause);
        }
        shouldClauses.addAll(abacPoliciesClauses);

        Map<String, Object> boolClause = new HashMap<>();
        if (shouldClauses.isEmpty()) {
            boolClause.put("must_not", getMap("match_all", new HashMap<>()));
        } else {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        return getMap("bool", boolClause);
    }

    private static List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions) {
        String policyQualifiedNamePrefix = null;
        if (persona != null && !persona.isEmpty()) {
            policyQualifiedNamePrefix = persona;
        } else if (purpose != null && !purpose.isEmpty()) {
            policyQualifiedNamePrefix = purpose;
        }

        //String user = RequestContext.getCurrentUser();
        String user = getCurrentUserName();
        LOG.info("Getting relevant policies for user: {}", user);

        RangerUserStore userStore = usersGroupsRolesStore.getUserStore();
        List<String> groups = getGroupsForUser(user, userStore);

        RangerRoles allRoles = usersGroupsRolesStore.getAllRoles();
        List<String> roles = getRolesForUser(user, allRoles);
        roles.addAll(getNestedRolesForUser(roles, allRoles));

        List<RangerPolicy> policies = new ArrayList<>();
        if ("atlas".equals(serviceName)) {
            policies = usersGroupsRolesStore.getResourcePolicies();
        } else if ("atlas_tag".equals(serviceName)) {
            policies = usersGroupsRolesStore.getTagPolicies();
        } else if ("atlas_abac".equals(serviceName)) {
            policies = usersGroupsRolesStore.getAbacPolicies();
        }

        if (CollectionUtils.isNotEmpty(policies)) {
            policies = getFilteredPoliciesForQualifiedName(policies, policyQualifiedNamePrefix);
            policies = getFilteredPoliciesForUser(policies, user, groups, roles);
            policies = getFilteredPoliciesForActions(policies, actions);
        }
        return policies;

    }

    private static List<Map<String, Object>> getDSLForAbacPolicies(List<RangerPolicy> policies) {
        List<String> filterCriteriaList = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            String filterCriteria = policy.getPolicyFilterCriteria();
            if (filterCriteria != null && !filterCriteria.isEmpty() ) {
                filterCriteriaList.add(filterCriteria);
            }
        }
        List<String> dslList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (String filterCriteria: filterCriteriaList) {
            JsonNode filterCriteriaNode = null;
            try {
                filterCriteriaNode = mapper.readTree(filterCriteria);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(entityFilterCriteriaNode, mapper);
                dslList.add(dsl.toString());
            }
        }
        List<Map<String, Object>> clauses = new ArrayList<>();
        for (String dsl: dslList) {
            String policyDSLBase64 = Base64.getEncoder().encodeToString(dsl.getBytes());;
            clauses.add(getMap("wrapper", getMap("query", policyDSLBase64)));
        }
        return clauses;
    }

    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies,String endOneGuid, String endTwoGuid) throws JsonProcessingException {
        return getDSLForRelationshipAbacPolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
    }

    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) throws JsonProcessingException {
        return getDSLForRelationshipAbacPolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
    }

    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) throws JsonProcessingException {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            if ("RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode filterCriteriaNode = mapper.readTree(filterCriteria);

                JsonNode endOneFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
                JsonNode endOneDsl = JsonToElasticsearchQuery.convertJsonToQuery(endOneFilterCriteriaNode, mapper);
                String endOneDslBase64 = Base64.getEncoder().encodeToString(endOneDsl.toString().getBytes());
                Map<String, Object> endOneFinalDsl = getMap("bool", getMap("filter", Arrays.asList(getMap("wrapper", getMap("query", endOneDslBase64)), endOneEntityDsl.toArray())));

                JsonNode endTwoFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
                JsonNode endTwoDsl = JsonToElasticsearchQuery.convertJsonToQuery(endTwoFilterCriteriaNode, mapper);
                String endTwoDslBase64 = Base64.getEncoder().encodeToString(endTwoDsl.toString().getBytes());
                Map<String, Object> endTwoFinalDsl = getMap("bool", getMap("filter", Arrays.asList(getMap("wrapper", getMap("query", endTwoDslBase64)), endTwoEntityDsl.toArray())));

                Map<String, Object> policyDsl = new HashMap<>();
                policyDsl.put("should", Arrays.asList(endOneFinalDsl, endTwoFinalDsl));
                policyDsl.put("minimum_should_match", 1);
                shouldClauses.add(getMap("bool", policyDsl));
            }
        }
        return shouldClauses;
    }

    private static Map<String, Object> getDSLForTagPolicies(List<RangerPolicy> policies) {
        // To reduce the number of clauses
        Set<String> allTags = new HashSet<>();

        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty()) {
                List<String> tags = policy.getResources().get("tag").getValues();
                if (!tags.isEmpty()) {
                    allTags.addAll(tags);
                }
            }
        }
        if (!allTags.isEmpty()) {
            return getDSLForTags(allTags);
        }
        return null;
    }

    private static Map<String, Object> getDSLForTagResourcePolicies(List<RangerPolicy> policies, String endOneGuid, String endTwoGuid) {
        return getDSLForRelationshipTagPolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
    }

    private static Map<String, Object> getDSLForTagResourcePolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) {
        return getDSLForRelationshipTagPolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
    }

    private static Map<String, Object> getDSLForRelationshipTagPolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) {
        // To reduce the number of clauses
        Set<String> allTags = new HashSet<>();
        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty()) {
                List<String> tags = policy.getResources().get("tag").getValues();
                if (!tags.isEmpty()) {
                    allTags.addAll(tags);
                }
            }
        }
        if (!allTags.isEmpty()) {
            Map<String, Object> tagsDsl = getDSLForTags(allTags);
            Map<String, Object> entityDsl = new HashMap<>();
            entityDsl.put("should", Arrays.asList(endOneEntityDsl.toArray(), endTwoEntityDsl.toArray()));
            entityDsl.put("minimum_should_match", 1);
            return getMap("bool", getMap("filter", Arrays.asList(getDSLForTags(allTags), getMap("bool", entityDsl))));
        }
        return null;
    }

    private static Map<String, Object> getDSLForTags(Set<String> tags){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);

        return getMap("bool", boolClause);
    }

    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, String endOneGuid, String endTwoGuid) {
        return getDSLForRelationshipResourcePolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
    }

    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) {
        return getDSLForRelationshipResourcePolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
    }

    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty() && "RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
                List<String> endOneEntities = policy.getResources().get("end-one-entity").getValues();
                List<String> endOneEntityTypes = policy.getResources().get("end-one-entity-type").getValues();
                List<String> endTwoEntities = policy.getResources().get("end-two-entity").getValues();
                List<String> endTwoEntityTypes = policy.getResources().get("end-two-entity-type").getValues();

                endOneEntities.remove("*");
                endOneEntityTypes.remove("*");
                endTwoEntities.remove("*");
                endTwoEntityTypes.remove("*");

                List<Map<String, Object>> endOneFilterList = new ArrayList<>();
                if (!endOneEntities.isEmpty() || !endOneEntityTypes.isEmpty()) {
                    Map<String, Object> endOneDsl = getDSLForResources(endOneEntities, endOneEntityTypes);
                    endOneFilterList.add(endOneDsl);
                }
                endOneFilterList.addAll(endOneEntityDsl);
                Map<String, Object> endOneFinalDsl = getMap("bool", getMap("filter", endOneFilterList));

                List<Map<String, Object>> endTwoFilterList = new ArrayList<>();
                if (!endTwoEntities.isEmpty() || !endTwoEntityTypes.isEmpty()) {
                    Map<String, Object> endTwoDsl = getDSLForResources(endTwoEntities, endTwoEntityTypes);
                    endOneFilterList.add(endTwoDsl);
                }
                endTwoFilterList.addAll(endTwoEntityDsl);
                Map<String, Object> endTwoFinalDsl = getMap("bool", getMap("filter", endTwoFilterList));

                Map<String, Object> policyDsl = new HashMap<>();
                policyDsl.put("should", Arrays.asList(endOneFinalDsl, endTwoFinalDsl));
                policyDsl.put("minimum_should_match", 1);
                shouldClauses.add(getMap("bool", policyDsl));
            }
        }
        return shouldClauses;
    }

    private static List<Map<String, Object>> getDSLForResourcePolicies(List<RangerPolicy> policies) {

        // To reduce the number of clauses
        List<String> combinedEntities = new ArrayList<>();
        List<String> combinedEntityTypes = new ArrayList<>();
        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty() && "ENTITY".equals(policy.getPolicyResourceCategory())) {
                List<String> entities = policy.getResources().get("entity").getValues();
                List<String> entityTypes = policy.getResources().get("entity-type").getValues();
                if (entities.contains("*") && entityTypes.contains("*")) {
                    Map<String, String> emptyMap = new HashMap<>();
                    shouldClauses.removeAll(shouldClauses);
                    shouldClauses.add(getMap("match_all",emptyMap));
                    break;
                }
                entities.remove("*");
                entityTypes.remove("*");
                if (!entities.isEmpty() && entityTypes.isEmpty()) {
                    combinedEntities.addAll(entities);
                } else if (entities.isEmpty() && !entityTypes.isEmpty()) {
                    combinedEntityTypes.addAll(entityTypes);
                } else if (!entities.isEmpty() && !entityTypes.isEmpty()) {
                    Map<String, Object> dslForPolicyResources = getDSLForResources(entities, entityTypes);
                    shouldClauses.add(dslForPolicyResources);
                }
            }
        }
        if (!combinedEntities.isEmpty()) {
            shouldClauses.add(getDSLForResources(combinedEntities, new ArrayList<>()));
        }
        if (!combinedEntityTypes.isEmpty()) {
            shouldClauses.add(getDSLForResources(new ArrayList<>(), combinedEntityTypes));
        }
        return shouldClauses;
    }

    private static Map<String, Object> getDSLForResources(List<String> entities, List<String> typeNames){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        List<String> termsQualifiedNames = new ArrayList<>();
        for (String entity: entities) {
            if (!entity.equals("*")) {
                if (entity.contains("*")) {
                    shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
                } else {
                    termsQualifiedNames.add(entity);
                }
            }
        }
        if (!termsQualifiedNames.isEmpty()) {
            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
        }

        Map<String, Object> boolClause = new HashMap<>();

        if (!shouldClauses.isEmpty()) {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        if (!typeNames.isEmpty()) {
            boolClause.put("filter", getMap("terms", getMap("__typeName.keyword", typeNames)));
        }

        return getMap("bool", boolClause);
    }

    static List<RangerPolicy> getFilteredPoliciesForQualifiedName(List<RangerPolicy> policies, String qualifiedNamePrefix) {
        if (qualifiedNamePrefix != null && !qualifiedNamePrefix.isEmpty()) {
            List<RangerPolicy> filteredPolicies = new ArrayList<>();
            for(RangerPolicy policy : policies) {
                if (policy.getName().startsWith(qualifiedNamePrefix)) {
                    filteredPolicies.add(policy);
                }
            }
            return filteredPolicies;
        }
        return policies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForActions(List<RangerPolicy> policies, List<String> actions) {
        List<RangerPolicy> filteredPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            if (!policy.getPolicyItems().isEmpty()) {
                RangerPolicy.RangerPolicyItem policyItem = policy.getPolicyItems().get(0);
                List<String> policyActions = new ArrayList<>();
                if (!policyItem.getAccesses().isEmpty()) {
                    for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                        policyActions.add(access.getType());
                    }
                }
                if (arrayListContains(policyActions, actions)) {
                    filteredPolicies.add(policy);
                }
            }
        }
        return filteredPolicies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForUser(List<RangerPolicy> policies, String user, List<String> groups, List<String> roles) {
        List<RangerPolicy> filterPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            if (!policy.getPolicyItems().isEmpty()) {
                RangerPolicy.RangerPolicyItem policyItem = policy.getPolicyItems().get(0);
                List<String> policyUsers = policyItem.getUsers();
                List<String> policyGroups = policyItem.getGroups();
                List<String> policyRoles = policyItem.getRoles();
                if (policyUsers.contains(user) || arrayListContains(policyGroups, groups) || arrayListContains(policyRoles, roles)) {
                    filterPolicies.add(policy);
                }
            }
        }
        return filterPolicies;
    }

    private static List<String> getGroupsForUser(String user, RangerUserStore userStore) {
        Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
        List<String> groups = new ArrayList<>();
        Set<String> groupsSet = userGroupMapping.get(user);
        if (groupsSet != null && !groupsSet.isEmpty()) {
            groups.addAll(groupsSet);
        }
        return groups;
    }

    private static List<String> getRolesForUser(String user, RangerRoles allRoles) {
        List<String> roles = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> users = role.getUsers();
            for (RangerRole.RoleMember roleUser: users) {
                if (roleUser.getName().equals(user)) {
                    roles.add(role.getName());
                }
            }
        }
        return roles;
    }

    private static List<String> getNestedRolesForUser(List<String> userRoles, RangerRoles allRoles) {
        List<String> ret = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> nestedRoles = role.getRoles();
            List<String> nestedRolesName = new ArrayList<>();
            for (RangerRole.RoleMember nestedRole : nestedRoles) {
                nestedRolesName.add(nestedRole.getName());
            }
            if (arrayListContains(userRoles, nestedRolesName)) {
             ret.add(role.getName());
            }
        }
        return ret;
    }

    private static boolean arrayListContains(List<String> listA, List<String> listB) {
        for (String listAItem : listA){
            if (listB.contains(listAItem)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, Object> getIndexsearchPreFilterDSL(String persona, String purpose) {
        RangerUserStore userStore = usersGroupsRolesStore.getUserStore();
        RangerRoles allRoles = usersGroupsRolesStore.getAllRoles();

        String user = getCurrentUserName();
        Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
        List<String> groups = new ArrayList<>();
        Set<String> groupsSet = userGroupMapping.get(user);
        if (groupsSet != null && !groupsSet.isEmpty()) {
            groups.addAll(groupsSet);
        }

        List<String> roles = new ArrayList<>();
        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
        for (RangerRole role : rangerRoles) {
            List<RangerRole.RoleMember> users = role.getUsers();
            for (RangerRole.RoleMember roleUser: users) {
                if (roleUser.getName().equals(user)) {
                    roles.add(role.getName());
                }
            }
        }

        List<String> actions = new ArrayList<>();
        actions.add("entity-read");
        actions.add("persona-asset-read");
        actions.add("domain-entity-read");

        List<Map<String, Object>> shouldList = new ArrayList<>();
        List<AtlasEntityHeader> apePolicies = new ArrayList<>();

        List<String> combinedEntities = new ArrayList<>();
        List<String> combinedEntityTypes = new ArrayList<>();
        Set<String> combinedTags = new HashSet<>();

        if (allPolicies == null) {
            allPolicies = getPolicies();
        }

        for (AtlasEntityHeader policy : allPolicies) {
            List<String> policyUsers = (List<String>) policy.getAttribute("policyUsers");
            List<String> policyGroups = (List<String>) policy.getAttribute("policyGroups");
            List<String> policyRoles = (List<String>) policy.getAttribute("policyRoles");
            List<String> policyActions = (List<String>) policy.getAttribute("policyActions");
            List<String> policyResources = (List<String>) policy.getAttribute("policyResources");
            String policyCategory = (String) policy.getAttribute("policyCategory");
            String policySubCategory = (String) policy.getAttribute("policySubCategory");
            String policyServiceName = (String) policy.getAttribute("policyServiceName");
            String policyResourceCategory = (String) policy.getAttribute("policyResourceCategory");
            String policyType = (String) policy.getAttribute("policyType");
            String policyQualifiedName = (String) policy.getAttribute("qualifiedName");

            if (persona != null && !policyQualifiedName.startsWith(persona)) {
                continue;
            } else if (purpose != null && !policyQualifiedName.startsWith(purpose)) {
                continue;
            }
            if (policyType != null && policyType.equals("allow")) {
                if (policyUsers.contains(user) || arrayListContains(policyGroups, groups) || arrayListContains(policyRoles, roles)) {
                    if (arrayListContains(policyActions, actions)) {
                        if (policyServiceName.equals("atlas_tag")) {
                            List<String> policyTags = getTagsFromResources(policyResources);
                            if (!policyTags.isEmpty()) {
                                combinedTags.addAll(policyTags);
                            }
                        } else if (policyServiceName.equals("ape")) {
                            apePolicies.add(policy);
                        }
                        else if (policyServiceName.equals("atlas") && (policyResourceCategory.equals("ENTITY") || policyResourceCategory.equals("CUSTOM"))) {
                            List<String> policyEntities = getEntitiesFromResources(policyResources, policyCategory, policySubCategory);
                            List<String> policyEntityTypes = getEntityTypesFromResources(policyResources);
                            if (!policyEntities.isEmpty() && policyEntityTypes.isEmpty()) {
                                combinedEntities.addAll(policyEntities);
                            } else if (policyEntities.isEmpty() && !policyEntityTypes.isEmpty()) {
                                combinedEntityTypes.addAll(policyEntityTypes);
                            } else if (!policyEntities.isEmpty() && !policyEntityTypes.isEmpty()) {
                                Map<String, Object> dslForPolicyResources = getDSLForPolicyResources(policyEntities, policyEntityTypes);
                                shouldList.add(dslForPolicyResources);
                            }
                        }
                    }
                }
            }
        }

        if (!combinedTags.isEmpty()) {
            Map<String, Object> dslForPolicyResources = getDSLForTagResources(combinedTags);
            shouldList.add(dslForPolicyResources);
        }
        if (!combinedEntities.isEmpty()) {
            Map<String, Object> DSLForEntityResources = getDSLForEntityResources(combinedEntities);
            if (DSLForEntityResources != null) {
                shouldList.add(DSLForEntityResources);
            }
        }
        if (!combinedEntityTypes.isEmpty()) {
            Map<String, Object> DSLForEntityTypeResources = getDSLForEntityTypeResources(combinedEntityTypes);
            if (DSLForEntityTypeResources != null) {
                shouldList.add(DSLForEntityTypeResources);
            }
        }
        if (!apePolicies.isEmpty()) {
            Map<String, Object> DSLForApePolicies = getDSLForApePolicies(apePolicies);
            if (DSLForApePolicies != null) {
                shouldList.add(DSLForApePolicies);
            }
        }

        Map<String, Object> allPreFiltersBoolClause = new HashMap<>();
        allPreFiltersBoolClause.put("should", shouldList);
        allPreFiltersBoolClause.put("minimum_should_match", 1);
        return allPreFiltersBoolClause;
    }

    private Map<String, Object> getDSLForApePolicies(List<AtlasEntityHeader> policies){
        if (policies!=null && !policies.isEmpty()) {
            List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
            List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
            List<Map<String, Object>> shouldClauseList = new ArrayList<>();
            for (String policyDSL: policyDSLArray) {
                String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
                shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
            }
            Map<String, Object> boolClause = new HashMap<>();
            boolClause.put("should", shouldClauseList);
            boolClause.put("minimum_should_match", 1);
            return getMap("bool", boolClause);
        }
        return null;
    }

    private List<String> getEntitiesFromResources(List<String> policyResources, String policyCategory, String policySubCategory){
        List<String> entities = new ArrayList<>();
        for(String resource: policyResources) {
            if (resource.contains("{USER}")) {
                resource.replace("{USER}", RequestContext.getCurrentUser());
            }
            if (resource.startsWith("entity:")) {
                if (!resource.substring(7).equals("*")) {
                    entities.add(resource.substring(7));
                    if (policyCategory.equals("persona")) {
                        if (policySubCategory.equals("metadata")) {
                            entities.add(resource.substring(7) + "/*");
                        } else if (policySubCategory.equals("glossary")) {
                            entities.add("*@" + resource.substring(7));
                        }
                    }
                }
            }
        }
        return entities;
    }

    private List<String> getEntityTypesFromResources(List<String> policyResources){
        List<String> entityTypes = new ArrayList<>();
        for(String resource: policyResources) {
            if (resource.startsWith("entity-type:") && !resource.equals("entity-type:*")) {
                entityTypes.add(resource.substring(12));
            }
        }
        return entityTypes;
    }

    private List<String> getTagsFromResources(List<String> policyResources){
        List<String> tags = new ArrayList<>();
        for(String resource: policyResources) {
            if (resource.startsWith("tag:") && !resource.equals("tag:*")) {
                tags.add(resource.substring(4));
            }
        }
        return tags;
    }

    private Map<String, Object> getDSLForEntityResources(List<String> entities){
        if (!entities.isEmpty()) {
            List<Map<String, Object>> shouldClauses = new ArrayList<>();
            List<String> termsQualifiedNames = new ArrayList<>();
            for (String entity: entities) {
                if (entity.contains("*")) {
                    shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
                } else {
                    termsQualifiedNames.add(entity);
                }
            }
            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));

            Map<String, Object> boolClause = new HashMap<>();
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
            return getMap("bool", boolClause);
        }
        return null;
    }

    private Map<String, Object> getDSLForEntityTypeResources(List<String> typeNames){
        if (!typeNames.isEmpty()) {
            return getMap("terms", getMap("__typeName.keyword", typeNames));
        }
        return null;
    }

    private Map<String, Object> getDSLForPolicyResources(List<String> entities, List<String> typeNames){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        List<String> termsQualifiedNames = new ArrayList<>();
        for (String entity: entities) {
            if (entity.contains("*")) {
                shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
            } else {
                termsQualifiedNames.add(entity);
            }
        }
        if (!termsQualifiedNames.isEmpty()) {
            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
        }

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);
        boolClause.put("filter", getMap("terms", getMap("__typeName.keyword", typeNames)));

        return getMap("bool", boolClause);
    }

    private Map<String, Object> getDSLForTagResources(Set<String> tags){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);

        return getMap("bool", boolClause);
    }

    public void refreshPolicies() {
//        if (isPolicyUpdated()) {}
        allPolicies = getPolicies();
    }

    private List<AtlasEntityHeader> getPolicies() {
        List<AtlasEntityHeader> ret = new ArrayList<>();
        int from = 0;
        int size = 1000;

        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", getMap("__typeName.keyword", POLICY_ENTITY_TYPE)));
        mustClauseList.add(mapOf("terms", getMap("policyServiceName", serviceNames)));
        mustClauseList.add(mapOf("match", getMap("__state", ACTIVE_STATE_VALUE)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
        dsl.put("from", from);
        dsl.put("size", size);

        indexSearchParams.setDsl(dsl);
        Set<String> attributes = new HashSet<>();
        attributes.add("policyType");
        attributes.add("policyServiceName");
        attributes.add("policyCategory");
        attributes.add("policySubCategory");
        attributes.add("policyUsers");
        attributes.add("policyGroups");
        attributes.add("policyRoles");
        attributes.add("policyActions");
        attributes.add("policyResources");
        attributes.add("policyResourceCategory");
        attributes.add("policyFilterCriteria");
        indexSearchParams.setAttributes(attributes);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = null;
        try {
            result = discoveryService.directIndexSearch(indexSearchParams);
            lastUpdatedTime = System.currentTimeMillis();
        } catch (AtlasBaseException e) {
            LOG.error("Error getting policies!", e);
        }
        if (result != null) {
            ret = result.getEntities();
        }
        return ret;
    }

    private List<String> getPolicyFilterCriteriaArray(List<AtlasEntityHeader> entityHeaders) {
        AtlasPerfMetrics.MetricRecorder getPolicyFilterCriteriaArrayMetrics = RequestContext.get().startMetricRecord("getPolicyFilterCriteriaArray");
        List<String> policyFilterCriteriaArray = new ArrayList<>();
        if (entityHeaders != null) {
            for (AtlasEntityHeader entity: entityHeaders) {
                String policyFilterCriteria = (String) entity.getAttribute("policyFilterCriteria");
                if (StringUtils.isNotEmpty(policyFilterCriteria)) {
                    policyFilterCriteriaArray.add(policyFilterCriteria);
                }
            }
        }
        RequestContext.get().endMetricRecord(getPolicyFilterCriteriaArrayMetrics);
        return policyFilterCriteriaArray;
    }

    private List<String> getPolicyDSLArray(List<String> policyFilterCriteriaArray) {
        AtlasPerfMetrics.MetricRecorder getPolicyDSLArrayMetrics = RequestContext.get().startMetricRecord("getPolicyDSLArray");
        List<String> policyDSLArray = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (String policyFilterCriteria: policyFilterCriteriaArray) {
            JsonNode policyFilterCriteriaNode = null;
            try {
                policyFilterCriteriaNode = mapper.readTree(policyFilterCriteria);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            JsonNode policyDSL = JsonToElasticsearchQuery.convertJsonToQuery(policyFilterCriteriaNode, mapper);
            policyDSLArray.add(policyDSL.toString());
        }
        RequestContext.get().endMetricRecord(getPolicyDSLArrayMetrics);
        return policyDSLArray;
    }

//    private boolean isPolicyUpdated() {
//        List<String> entityUpdateToWatch = new ArrayList<>();
//        entityUpdateToWatch.add(POLICY_ENTITY_TYPE);
//        entityUpdateToWatch.add(PERSONA_ENTITY_TYPE);
//        entityUpdateToWatch.add(PURPOSE_ENTITY_TYPE);
//
//        AuditSearchParams parameters = new AuditSearchParams();
//        Map<String, Object> dsl = getMap("size", 1);
//
//        List<Map<String, Object>> mustClauseList = new ArrayList<>();
//        mustClauseList.add(getMap("terms", getMap("typeName", entityUpdateToWatch)));
//
//        lastUpdatedTime = lastUpdatedTime == -1 ? 0 : lastUpdatedTime;
//        mustClauseList.add(getMap("range", getMap("timestamp", getMap("gte", lastUpdatedTime))));
//
//        dsl.put("query", getMap("bool", getMap("must", mustClauseList)));
//
//        parameters.setDsl(dsl);
//
//        try {
//            EntityAuditSearchResult result = auditRepository.searchEvents(parameters.getQueryString());
//
//            if (result == null || CollectionUtils.isEmpty(result.getEntityAudits())) {
//                return false;
//            }
//        } catch (AtlasBaseException e) {
//            LOG.error("ERROR in getPoliciesIfUpdated while fetching entity audits {}: ", e.getMessage());
//            return true;
//        }
//        return true;
//    }

//    private List<AtlasEntityHeader> getRelevantPolicies(String user, String action) throws AtlasBaseException {
//        AtlasPerfMetrics.MetricRecorder getRelevantPoliciesMetrics = RequestContext.get().startMetricRecord("getRelevantPolicies");
//        List<AtlasEntityHeader> ret = new ArrayList<>();
//
//        IndexSearchParams indexSearchParams = new IndexSearchParams();
//        Map<String, Object> dsl = new HashMap<>();
//
//        List mustClauseList = new ArrayList();
//        mustClauseList.add(mapOf("term", getMap("__typeName.keyword", POLICY_ENTITY_TYPE)));
//        mustClauseList.add(mapOf("term", getMap("policyUsers", user)));
//        mustClauseList.add(mapOf("term", getMap("policyActions", action)));
//        mustClauseList.add(mapOf("term", getMap("policyServiceName", POLICY_SERVICE_NAME_APE)));
//
//        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
//
//        indexSearchParams.setDsl(dsl);
//        Set<String> attributes = new HashSet<>();
//        attributes.add("policyFilterCriteria");
//        indexSearchParams.setAttributes(attributes);
//        indexSearchParams.setSuppressLogs(true);
//
//        AtlasSearchResult result = discoveryService.directIndexSearch(indexSearchParams);
//        if (result != null) {
//            ret = result.getEntities();
//        }
//        RequestContext.get().endMetricRecord(getRelevantPoliciesMetrics);
//
//        return ret;
//    }

//    private String getAccessControlDSL(List<String> policyDSLArray, String entityQualifiedName, String entityTypeName) {
//        try {
//            List<Map<String, Object>> mustClauseList = new ArrayList<>();
//            Map<String, List> boolObjects = new HashMap<>();
//            List<Map<String, Object>> filterClauseList = new ArrayList<>();
//            filterClauseList.add(getMap("term", getMap("qualifiedName", entityQualifiedName)));
//            filterClauseList.add(getMap("term", getMap("__typeName.keyword", entityTypeName)));
//            boolObjects.put("filter", filterClauseList);
//            mustClauseList.add(getMap("bool", boolObjects));
//
//            List<Map<String, Object>> shouldClauseList = new ArrayList<>();
//            for (String policyDSL: policyDSLArray) {
//                String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
//                shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
//            }
//            Map<String, Object> boolClause = new HashMap<>();
//            boolClause.put("should", shouldClauseList);
//            boolClause.put("minimum_should_match", 1);
//            mustClauseList.add(getMap("bool", boolClause));
//            JsonNode queryNode = mapper.valueToTree(getMap("query", getMap("bool", getMap("must", mustClauseList))));
//            return queryNode.toString();
//
//        } catch (Exception e) {
//            LOG.error("Error -> addPreFiltersToSearchQuery!", e);
//            return null;
//        }
//    }
//
//    private void createUserPolicyMap() {
//        try {
//            JsonNode jsonTree = mapper.readTree(policiesString);
//            int treeSize = jsonTree.size();
//            for (int i = 0; i < treeSize; i++) {
//                JsonNode node = jsonTree.get(i);
//                String users = node.get("properties").get("subjects").get("properties").get("users").get("items").get("pattern").asText();
//                List<String> usersList = Arrays.asList(users.split("\\|"));
//                for (String user : usersList) {
//                    if (userPoliciesMap.get(user) == null) {
//                        Set<Integer> policySet = new HashSet<>();
//                        policySet.add(i);
//                        userPoliciesMap.put(user, policySet);
//                    } else {
//                        userPoliciesMap.get(user).add(i);
//                    }
//                }
//                LOG.info("userPoliciesMap");
//                LOG.info(String.valueOf(userPoliciesMap));
//            }
//        } catch (Exception e) {
//            LOG.error("Error -> createUserPolicyMap!", e);
//        }
//    }
//
//    private String getPolicies() throws IOException {
//        String atlasHomeDir = System.getProperty("atlas.home");
//        String atlasHome = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
//        File policiesFile = Paths.get(atlasHome, "policies", "jpolicies.json").toFile();
//        return new String(Files.readAllBytes(policiesFile.toPath()), StandardCharsets.UTF_8);
//    }
//
//    public boolean isAccessAllowed(String user, String action, String entityQualifiedName, String entityTypeName){
//        List<AtlasEntityHeader> policies = new ArrayList<>();
//        Integer count = null;
//        try {
//            policies = getRelevantPolicies(user, action);
//        } catch (AtlasBaseException e) {
//            e.printStackTrace();
//        }
//        List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
//        List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
//        if (!policyDSLArray.isEmpty()) {
//            String query = getAccessControlDSL(policyDSLArray, entityQualifiedName, entityTypeName);
//            count = getCountFromElasticsearch(query);
//        }
//
//        if (count != null && count > 0) {
//            return true;
//        }
//        return false;
//    }
//
//    public boolean isAccessAllowed(Map<String, Object> entity, String user){
//        try {
//            long startTime = System.nanoTime();
//            JsonNode jsonTree = mapper.readTree(policiesString);
//            String entityString = mapper.writeValueAsString(entity);
//            JsonNode node = getJsonNodeFromStringContent(entityString);
//            Set<Integer> userPolicies = userPoliciesMap.get(user);
//            boolean error = false;
//            for (Integer policyId : userPolicies) {
//                JsonNode jsonNode = jsonTree.get(policyId);
//                JsonSchema schema = getJsonSchemaFromJsonNode(jsonNode);
//                Set<ValidationMessage> errors = schema.validate(node);
//                if (errors != null && errors.size() > 0) {
//                    error = true;
//                }
//                LOG.info("Error Count -> ");
//                LOG.info(String.valueOf(errors.size()));
//            }
//            long endTime = System.nanoTime();
//            long finalDuration = ((endTime - startTime)/1000000);
//            LOG.info("finalDuration");
//            LOG.info(String.valueOf(finalDuration));
//            return userPolicies.size() > 0 &&  !error;
//        } catch (Exception e) {
//            LOG.info("error!");
//            return false;
//        }
//
//    }
//
//    private String jsonSchemaToElasticsearchDSL(String jsonSchema) {
//        // Create ObjectMapper
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        // Parse JSON 2 string
//        JsonNode jsonSchemaNode = null;
//        try {
//            jsonSchemaNode = objectMapper.readTree(jsonSchema);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//
//        // Create JSON 1 structure
//        ObjectNode elasticsearchDSL = objectMapper.createObjectNode();
//        ObjectNode elasticsearchDSLBool = objectMapper.createObjectNode();
//        elasticsearchDSL.set("bool", elasticsearchDSLBool);
//        ArrayNode mustNotNode = elasticsearchDSLBool.putArray("must_not");
//        ArrayNode filterNode = elasticsearchDSLBool.putArray("filter");
//
//        // Iterate over attributes in JSON 2 and convert to JSON 1
//        jsonSchemaNode.fields().forEachRemaining(entry -> {
//            String attributeName = entry.getKey();
//            JsonNode attributeNode = entry.getValue();
//
//            // Create condition based on attribute type and pattern
//            if (attributeNode.has("pattern")) {
//                String pattern = attributeNode.get("pattern").asText();
//                if (pattern.startsWith("^(?!") && pattern.endsWith("$).*$")) {
//                    // Convert to must_not condition
//                    ObjectNode termNode = mustNotNode.addObject();
//                    termNode.putObject("term").put(attributeName, pattern.substring(4, pattern.length() - 5));
//                } else if (pattern.startsWith("^") && pattern.endsWith("$")) {
//                    // Convert to filter condition
//                    ObjectNode termNode = filterNode.addObject();
//                    termNode.putObject("term").put(attributeName, pattern.substring(1, pattern.length() - 1));
//                } else if (pattern.startsWith(".*")) {
//                    // Convert to filter wildcard condition
//                    ObjectNode wildcardNode = filterNode.addObject().putObject("wildcard");
//                    wildcardNode.putObject(attributeName).put("value","*" + pattern.substring(2));
//                } else if (pattern.endsWith(".*")) {
//                    // Convert to filter wildcard condition
//                    ObjectNode wildcardNode = filterNode.addObject().putObject("wildcard");
//                    wildcardNode.putObject(attributeName).put("value", pattern.substring(0, pattern.length() - 2) + "*");
//                }
//            }
//        });
//
//        // Convert JSON 1 to string
//        return elasticsearchDSL.toString();
//    }
//
//    private Integer getCountFromElasticsearch(String query) {
//        RestClient restClient = getLowLevelClient();
//        AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
//        Map<String, Object> elasticsearchResult = null;
//        try {
//            elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(query);
//        } catch (AtlasBaseException e) {
//            e.printStackTrace();
//        }
//        Integer count = null;
//        if (elasticsearchResult!=null) {
//            count = (Integer) elasticsearchResult.get("total");
//        }
//        return count;
//    }
//
//    protected JsonNode getJsonNodeFromStringContent(String content) throws IOException {
//        return mapper.readTree(content);
//    }
//
//    protected JsonSchema getJsonSchemaFromJsonNode(JsonNode jsonNode) {
//        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
//        return factory.getSchema(jsonNode);
//    }
//
//    protected com.networknt.schema.JsonSchema getJsonSchemaFromStringContent(String schemaContent) {
//        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
//        return factory.getSchema(schemaContent);
//    }
//
//    public List<Map<String, Object>> getAuthPreFilterDSL(String user, String action){
//        List<AtlasEntityHeader> policies = new ArrayList<>();
//        try {
//            policies = getRelevantPolicies(user, action);
//        } catch (AtlasBaseException e) {
//            e.printStackTrace();
//        }
//        List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
//        List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
//        List<Map<String, Object>> shouldClauseList = new ArrayList<>();
//        AtlasPerfMetrics.MetricRecorder base64EncodingMetrics = RequestContext.get().startMetricRecord("base64Encoding");
//        for (String policyDSL: policyDSLArray) {
//            String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
//            shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
//        }
//        RequestContext.get().endMetricRecord(base64EncodingMetrics);
//        return shouldClauseList;
//    }

    public static String getCurrentUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null ? auth.getName() : "";
    }

}
