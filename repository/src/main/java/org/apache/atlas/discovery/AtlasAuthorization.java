package org.apache.atlas.discovery;

import org.apache.atlas.authorizer.authorizers.AuthorizerCommon;
//import org.apache.atlas.model.audit.AuditSearchParams;
//import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class AtlasAuthorization {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorization.class);

    //private EntityDiscoveryService discoveryService;

    private static AtlasTypeRegistry typeRegistry;
    private static EntityGraphRetriever entityRetriever;

    //private static AtlasAuthorization  atlasAuthorization;
    //private static UsersGroupsRolesStore  usersGroupsRolesStore;
    private List<String> serviceNames = new ArrayList<>();
    private static Map<String, String> esEntityAttributeMap = new HashMap<>();
    private static final String POLICY_TYPE_ALLOW = "allow";
    private static final String POLICY_TYPE_DENY = "deny";

    /*public static AtlasAuthorization getInstance(EntityDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
        synchronized (AtlasAuthorization.class) {
            if (atlasAuthorization == null) {
                atlasAuthorization = new AtlasAuthorization(discoveryService, typeRegistry);
            }
            return atlasAuthorization;
        }
    }

    public static AtlasAuthorization getInstance() {
        if (atlasAuthorization != null) {
            return atlasAuthorization;
        }
        return null;
    }*/

    @Inject
    //public AtlasAuthorization (EntityDiscoveryService discoveryService, AtlasTypeRegistry typeRegistry) {
    public AtlasAuthorization (AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        try {
            //this.discoveryService = discoveryService;

            this.typeRegistry = typeRegistry;
            this.entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);

            //AtlasAuthorization.usersGroupsRolesStore = UsersGroupsRolesStore.getInstance();
            AuthorizerCommon.setTypeRegistry(typeRegistry);
            AuthorizerCommon.setEntityRetriever(entityRetriever);

            serviceNames.add("atlas");
            serviceNames.add("atlas_tag");
            serviceNames.add("ape");

            esEntityAttributeMap.put("__typeName.keyword", "__typeName");

            LOG.info("==> AtlasAuthorization");
        } catch (Exception e) {
            LOG.error("==> AtlasAuthorization -> Error!");
        }
    }

    /*public static void verifyAccess(String guid, String action) throws AtlasBaseException {
        String userName = getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (!isAccessAllowed(guid, action)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, userName, action + ":" + guid);
            }
        } catch (AtlasBaseException e) {
            throw e;
        }
    }

    public static void verifyAccess(String action, String endOneGuid, String endTwoGuid) throws AtlasBaseException {
        String userName = getCurrentUserName();

        if (StringUtils.isEmpty(userName) || RequestContext.get().isImportInProgress()) {
            return;
        }

        try {
            if (!isRelationshipAccessAllowed(action, endOneGuid, endTwoGuid)) {
                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), endOneGuid + "|" + endTwoGuid);
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

    private static boolean isRelationshipAccessAllowed(String action, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        boolean deny = checkRelationshipAccessAllowed(action, endOneEntity, endTwoEntity, POLICY_TYPE_DENY);
        if (deny) {
            return false;
        }
        return checkRelationshipAccessAllowed(action, endOneEntity, endTwoEntity, POLICY_TYPE_ALLOW);
    }

    public static boolean checkRelationshipAccessAllowed(String action, AtlasEntityHeader endOneEntity,
                                                         AtlasEntityHeader endTwoEntity, String policyType) throws AtlasBaseException {
        //Relationship add, update, remove access check in memory
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("isRelationshipAccessAllowed."+policyType);

        try {
            List<RangerPolicy> policies = getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
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
                if (filterCriteriaNode != null && filterCriteriaNode.get("entityOneEntity") != null) {
                    JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entityOneEntity");
                    eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(endOneEntity));

                    if (eval) {
                        entityFilterCriteriaNode = filterCriteriaNode.get("entityTwoEntity");
                        eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(endOneEntity));
                    }
                }
                ret = ret || eval;
                if (ret) {
                    break;
                }
            }

            if (!ret) {
                List<RangerPolicy> tagPolicies = getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action), policyType);
                List<RangerPolicy> resourcePolicies = getRelevantPolicies(null, null, "atlas", Collections.singletonList(action), policyType);

                tagPolicies.addAll(resourcePolicies);

                ret = validateResourcesForCreateRelationship(tagPolicies, endOneEntity, endTwoEntity);
            }

            return ret;
        } catch (NullPointerException e) {
            return false;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static boolean isRelationshipAccessAllowed(String action, String endOneGuid, String endTwoGuid) throws AtlasBaseException {
        //Relationship update, remove access check with ES query
        if (endOneGuid == null || endTwoGuid == null) {
            return false;
        }

        try {
            Map<String, Object> dsl = getElasticsearchDSLForRelationshipActions(Arrays.asList(action), endOneGuid, endTwoGuid);
            ObjectMapper mapper = new ObjectMapper();
            String dslString = mapper.writeValueAsString(dsl);
            RestClient restClient = getLowLevelClient();
            AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
            Map<String, Object> elasticsearchResult = null;
            elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(dslString);
            Integer count = null;
            if (elasticsearchResult!=null) {
                count = (Integer) elasticsearchResult.get("total");
            }
            if (count != null && count == 2) {
                List<Map<String, Object>> docs = (List<Map<String, Object>>) elasticsearchResult.get("data");
                List<String> matchedClausesEndOne = new ArrayList<>();
                List<String> matchedClausesEndTwo = new ArrayList<>();
                for (Map<String, Object> doc : docs) {
                    List<String> matched_queries = (List<String>) doc.get("matched_queries");
                    if (matched_queries != null && !matched_queries.isEmpty()) {
                        Map<String, Object> source = (Map<String, Object>) doc.get("_source");
                        String guid = (String) source.get("__guid");
                        if (endOneGuid.equals(guid)) {
                            for (String matched_query : matched_queries) {
                                if (matched_query.equals("tag-clause")) {
                                    matchedClausesEndOne.add("tag-clause");
                                } else if (matched_query.startsWith("end-one-")) {
                                    matchedClausesEndOne.add(matched_query.substring(8));
                                }
                            }
                        } else {
                            for (String matched_query : matched_queries) {
                                if (matched_query.equals("tag-clause")) {
                                    matchedClausesEndTwo.add("tag-clause");
                                } else if (matched_query.startsWith("end-two-")) {
                                    matchedClausesEndTwo.add(matched_query.substring(8));
                                }
                            }
                        }
                    }
                }
                if (arrayListContains(matchedClausesEndOne, matchedClausesEndTwo)) {
                    return true;
                }
            }
            LOG.info(dslString);
        } catch (JsonProcessingException e) {
            return false;
        }
        return false;
    }

    public static boolean isCreateAccessAllowed(AtlasEntity entity, String action) {
        boolean deny = isCreateAccessAllowed(entity, action, POLICY_TYPE_DENY);
        if (deny) {
            return false;
        }
        return isCreateAccessAllowed(entity, action, POLICY_TYPE_ALLOW);
    }

    public static boolean isCreateAccessAllowed(AtlasEntity entity, String action, String policyType) {
        List<RangerPolicy> policies = getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
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
            List<RangerPolicy> tagPolicies = getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action), policyType);
            List<RangerPolicy> resourcePolicies = getRelevantPolicies(null, null, "atlas", Collections.singletonList(action), policyType);

            tagPolicies.addAll(resourcePolicies);

            ret = validateResourcesForCreateEntity(tagPolicies, entity);
        }

        return ret;
    }

    private static boolean validateResourcesForCreateEntity(List<RangerPolicy> resourcePolicies, AtlasEntity entity) {
        RangerPolicy matchedPolicy = null;
        Set<String> entityTypes = getTypeAndSupertypesList(entity.getTypeName());

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
                        boolean match = entityTypes.stream().anyMatch(assetType -> values.stream().anyMatch(policyAssetType -> assetType.matches(policyAssetType.replace("*", ".*"))));

                        if (!match) {
                            resourcesMatched = false;
                            break;
                        }
                    }

                    if ("entity".equals(resource)) {
                        String assetQualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
                        Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x
                                .replace("{USER}", getCurrentUserName())
                                .replace("*", ".*")))
                                .findFirst();

                        if (!match.isPresent()) {
                            resourcesMatched = false;
                            break;
                        }
                    }

                    //for tag based policy
                    if ("tag".equals(resource)) {
                        if (entity.getClassifications() == null || entity.getClassifications().isEmpty()) {
                            //since entity does not have tags at all, it should not pass this evaluation
                            resourcesMatched = false;
                            break;
                        }

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
                    matchedPolicy = rangerPolicy;
                    LOG.info("Matched with policy: {}:{}", matchedPolicy.getName(), matchedPolicy.getGuid());
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean validateResourcesForCreateRelationship(List<RangerPolicy> resourcePolicies, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) {
        RangerPolicy matchedPolicy = null;

        Set<String> endOneEntityTypes = getTypeAndSupertypesList(endOneEntity.getTypeName());
        Set<String> endTwoEntityTypes = getTypeAndSupertypesList(endTwoEntity.getTypeName());

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
                matchedPolicy = rangerPolicy;
                LOG.info("Matched with policy: {}:{}", matchedPolicy.getName(), matchedPolicy.getGuid());
                return true;

            } else {
                boolean resourcesMatched = true;

                for (String resource : resources.keySet()) {
                    List<String> values = resources.get(resource).getValues();

                    if ("end-one-entity-type".equals(resource)) {
                        if (!values.contains(("*"))) {
                            boolean match = endOneEntityTypes.stream().anyMatch(assetType -> values.stream().anyMatch(policyAssetType -> assetType.matches(policyAssetType.replace("*", ".*"))));

                            if (!match) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    if ("end-two-entity-type".equals(resource)) {
                        if (!values.contains(("*"))) {
                            boolean match = endTwoEntityTypes.stream().anyMatch(assetType -> values.stream().anyMatch(policyAssetType -> assetType.matches(policyAssetType.replace("*", ".*"))));

                            if (!match) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    if ("end-one-entity".equals(resource)) {
                        if (!values.contains(("*"))) {
                            String assetQualifiedName = (String) endOneEntity.getAttribute(QUALIFIED_NAME);
                            Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x
                                            .replace("{USER}", getCurrentUserName())
                                            .replace("*", ".*")))
                                    .findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    if ("end-two-entity".equals(resource)) {
                        if (!values.contains(("*"))) {
                            String assetQualifiedName = (String) endTwoEntity.getAttribute(QUALIFIED_NAME);
                            Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x
                                            .replace("{USER}", getCurrentUserName())
                                            .replace("*", ".*")))
                                    .findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    //for tag based policy
                    if ("end-one-entity-classification".equals(resource)) {
                        if (!values.contains(("*"))) {
                            if (endOneEntity.getClassifications() == null || endOneEntity.getClassifications().isEmpty()) {
                                //since entity does not have tags at all, it should not pass this evaluation
                                resourcesMatched = false;
                                break;
                            }

                            List<String> assetTags = endOneEntity.getClassifications().stream().map(x -> x.getTypeName()).collect(Collectors.toList());

                            boolean match = assetTags.stream().anyMatch(assetTag -> values.stream().anyMatch(policyAssetType -> assetTag.matches(policyAssetType.replace("*", ".*"))));

                            if (!match) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    if ("end-two-entity-classification".equals(resource)) {
                        if (!values.contains(("*"))) {
                            if (endTwoEntity.getClassifications() == null || endTwoEntity.getClassifications().isEmpty()) {
                                //since entity does not have tags at all, it should not pass this evaluation
                                resourcesMatched = false;
                                break;
                            }

                            List<String> assetTags = endTwoEntity.getClassifications().stream().map(x -> x.getTypeName()).collect(Collectors.toList());

                            boolean match = assetTags.stream().anyMatch(assetTag -> values.stream().anyMatch(policyAssetType -> assetTag.matches(policyAssetType.replace("*", ".*"))));

                            if (!match) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }
                }

                if (resourcesMatched) {
                    matchedPolicy = rangerPolicy;
                    LOG.info("Matched with policy: {}:{}", matchedPolicy.getName(), matchedPolicy.getGuid());
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

        Set<String> assetTypes = getTypeAndSupertypesList(entity.getTypeName());

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

//                List<String> attributeValues = new ArrayList<>();
//                if (operator.equals("IN") || operator.equals("NOT_IN")) {
//                    for (JsonNode valueNode : crit.get("attributeValue")) {
//                        ObjectMapper mapper = new ObjectMapper();
//                        String value = null;
//                        try {
//                            value = mapper.treeToValue(valueNode, String.class);
//                        } catch (JsonProcessingException e) {
//                            e.printStackTrace();
//                        }
//                        attributeValues.add(value);
//                    }
//                }


                if (attributeName.endsWith(".text")) {
                    attributeName.replace(".text", "");
                } else if (attributeName.endsWith(".keyword")) {
                    attributeName.replace(".keyword", "");
                }

                List<String> entityAttributeValues = new ArrayList<>();

                if (attributeName.equals("__superTypeNames")) {
                    entityAttributeValues.addAll(assetTypes);

                } if (attributeName.equals("__typeName")) {
                    entityAttributeValues.add(entity.getTypeName());

                } if (attributeName.equals("__guid")) {
                    entityAttributeValues.add(entity.getGuid());

                } else if (attributeName.equals("__traitNames")) {
                    List<AtlasClassification> atlasClassifications = entity.getClassifications();
                    if (atlasClassifications != null && !atlasClassifications.isEmpty()) {
                        for (AtlasClassification atlasClassification : atlasClassifications) {
                            entityAttributeValues.add(atlasClassification.getTypeName());
                        }
                    }
                } else if (attributeName.equals("__meaningNames")) {
                    List<AtlasTermAssignmentHeader> atlasMeanings = entity.getMeanings();
                    for (AtlasTermAssignmentHeader atlasMeaning : atlasMeanings) {
                        entityAttributeValues.add(atlasMeaning.getDisplayText());
                    }
                } else {
                    String typeName = entity.getTypeName();
                    boolean isArrayOfPrimitiveType = false;
                    boolean isArrayOfEnum = false;
                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
                    AtlasStructType.AtlasAttribute atlasAttribute = entityType.getAttribute(attributeName);
                    if (atlasAttribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
                        AtlasArrayType attributeType = (AtlasArrayType) atlasAttribute.getAttributeType();
                        AtlasType elementType = attributeType.getElementType();
                        isArrayOfPrimitiveType = elementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                        isArrayOfEnum = elementType.getTypeCategory().equals(TypeCategory.ENUM);
                    }

                    if (entity.getAttribute(attributeName) != null) {
                        if (isArrayOfEnum || isArrayOfPrimitiveType) {
                            entityAttributeValues.addAll((Collection<? extends String>) entity.getAttribute(attributeName));
                        } else {
                            entityAttributeValues.add((String) entity.getAttribute(attributeName));
                        }
                    }
                }

                if (operator.equals("EQUALS") && entityAttributeValues.contains(attributeValue)) {
                    evaluation = true;
                }
                if ((operator.equals("STARTS_WITH") && listStartsWith(attributeValue, entityAttributeValues))) {
                    evaluation = true;
                }
                if ((operator.equals("ENDS_WITH") && listEndsWith(attributeValue, entityAttributeValues))) {
                    evaluation = true;
                }
                if ((operator.equals("NOT_EQUALS") && !entityAttributeValues.contains(attributeValue))) {
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

    public static Map<String, Object> getElasticsearchDSLForRelationshipActions(List<String> actions, String endOneGuid, String endTwoGuid) throws JsonProcessingException {
        List<Map<String, Object>> policiesClauses = new ArrayList<>();
        List<RangerPolicy> resourcePolicies = getRelevantPolicies(null, null, "atlas", actions, POLICY_TYPE_ALLOW);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForRelationshipResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = getRelevantPolicies(null, null, "atlas_tag", actions, POLICY_TYPE_ALLOW);
        List<Map<String, Object>> tagPoliciesClauses = getDSLForRelationshipTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = getRelevantPolicies(null, null, "atlas_abac", actions, POLICY_TYPE_ALLOW);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForRelationshipAbacPolicies(abacPolicies);

        policiesClauses.addAll(resourcePoliciesClauses);
        policiesClauses.addAll(tagPoliciesClauses);
        policiesClauses.addAll(abacPoliciesClauses);

        List<Map<String, Object>> clauses = new ArrayList<>();

        Map<String, Object> policiesBoolClause = new HashMap<>();
        if (policiesClauses.isEmpty()) {
            policiesBoolClause.put("must_not", getMap("match_all", new HashMap<>()));
        } else {
            policiesBoolClause.put("should", policiesClauses);
            policiesBoolClause.put("minimum_should_match", 1);
        }
        clauses.add(getMap("bool", policiesBoolClause));

        Map<String, Object> entitiesBoolClause = new HashMap<>();
        List<Map<String, Object>> entityClauses = new ArrayList<>();
        entityClauses.add(getMap("term", getMap("__guid", endOneGuid)));
        entityClauses.add(getMap("term", getMap("__guid", endTwoGuid)));
        entitiesBoolClause.put("should", entityClauses);
        entitiesBoolClause.put("minimum_should_match", 1);
        clauses.add(getMap("bool", entitiesBoolClause));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("filter", clauses);

        return getMap("query", getMap("bool", boolClause));
    }

    public static Map<String, Object> getElasticsearchDSL(String persona, String purpose, List<String> actions) {
        Map<String, Object> allowDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_ALLOW);
        Map<String, Object> denyDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_DENY);
        Map<String, Object> finaDsl = new HashMap<>();
        if (allowDsl != null) {
            finaDsl.put("filter", allowDsl);
        }
        if (denyDsl != null) {
            finaDsl.put("must_not", denyDsl);
        }
        return getMap("bool", finaDsl);
    }

    public static Map<String, Object> getElasticsearchDSLForPolicyType(String persona, String purpose, List<String> actions, String policyType) {
        List<RangerPolicy> resourcePolicies = getRelevantPolicies(persona, purpose, "atlas", actions, policyType);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = getRelevantPolicies(persona, purpose, "atlas_tag", actions, policyType);
        Map<String, Object> tagPoliciesClause = getDSLForTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = getRelevantPolicies(persona, purpose, "atlas_abac", actions, policyType);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForAbacPolicies(abacPolicies);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.addAll(resourcePoliciesClauses);
        if (tagPoliciesClause != null) {
            shouldClauses.add(tagPoliciesClause);
        }
        shouldClauses.addAll(abacPoliciesClauses);

        Map<String, Object> boolClause = new HashMap<>();
        if (shouldClauses.isEmpty()) {
            if (POLICY_TYPE_ALLOW.equals(policyType)) {
                boolClause.put("must_not", getMap("match_all", new HashMap<>()));
            } else {
                return null;
            }

        } else {
            boolClause.put("should", shouldClauses);
            boolClause.put("minimum_should_match", 1);
        }

        return getMap("bool", boolClause);

    }

    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies) throws JsonProcessingException {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            if ("RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode filterCriteriaNode = mapper.readTree(filterCriteria);

                List<String> relationshipEnds = new ArrayList<>();
                relationshipEnds.add("end-one");
                relationshipEnds.add("end-two");

                for (String relationshipEnd : relationshipEnds) {
                    JsonNode endFilterCriteriaNode = filterCriteriaNode.get(relationshipEnd == "end-one" ? "endOneEntity" : "endTwoEntity");
                    JsonNode Dsl = JsonToElasticsearchQuery.convertJsonToQuery(endFilterCriteriaNode, mapper);
                    String DslBase64 = Base64.getEncoder().encodeToString(Dsl.toString().getBytes());
                    String clauseName = relationshipEnd + "-" + policy.getGuid();
                    Map<String, Object> wrapperMap = new HashMap<>();
                    wrapperMap.put("_name", clauseName);
                    wrapperMap.put("query", DslBase64);
                    shouldClauses.add(wrapperMap);
                }
            }
        }
        return shouldClauses;
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

    private static List<Map<String, Object>> getDSLForRelationshipTagPolicies(List<RangerPolicy> policies) {
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

        List<Map<String, Object>> clauses = new ArrayList<>();

        if (!allTags.isEmpty()) {
            Map<String, Object> termsMapA = new HashMap<>();
            termsMapA.put("_name", "tag-clause");
            termsMapA.put("terms", getMap("__traitNames", allTags));
            clauses.add(termsMapA);

            Map<String, Object> termsMapB = new HashMap<>();
            termsMapB.put("_name", "tag-clause");
            termsMapB.put("terms", getMap("__propagatedTraitNames", allTags));
            clauses.add(termsMapB);
        }
        return clauses;
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

    private static Map<String, Object> getDSLForTags(Set<String> tags){
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("should", shouldClauses);
        boolClause.put("minimum_should_match", 1);

        return getMap("bool", boolClause);
    }

    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies) {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty() && "RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
                List<String> relationshipEnds = new ArrayList<>();
                relationshipEnds.add("end-one");
                relationshipEnds.add("end-two");

                for (String relationshipEnd : relationshipEnds) {
                    String clauseName = relationshipEnd + "-" + policy.getGuid();
                    String entityParamName = relationshipEnd + "-entity";
                    String entityTypeParamName = relationshipEnd + "-entity-type";
                    String entityClassificationParamName = relationshipEnd + "-entity-classification";

                    List<String> entities = policy.getResources().get(entityParamName).getValues();
                    List<String> entityTypesRaw = policy.getResources().get(entityTypeParamName).getValues();
                    Set<String> entityTypes = new HashSet<>();
                    entityTypesRaw.forEach(x -> entityTypes.addAll(getTypeAndSupertypesList(x)));

                    List<String> entityClassifications = policy.getResources().get(entityClassificationParamName).getValues();
                    if (entities.contains("*") && entityTypes.contains("*") && entityClassifications.contains("*")) {
                        shouldClauses.add(getMap("match_all", getMap("_name", clauseName)));
                    } else {
                        Map<String, Object> dslForPolicyResources = getDSLForResources(entities, entityTypes, entityClassifications, clauseName);
                        shouldClauses.add(dslForPolicyResources);
                    }
                }
            }
        }
        return shouldClauses;
    }

    private static List<Map<String, Object>> getDSLForResourcePolicies(List<RangerPolicy> policies) {

        // To reduce the number of clauses
        List<String> combinedEntities = new ArrayList<>();
        Set<String> combinedEntityTypes = new HashSet<>();
        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (!policy.getResources().isEmpty() && "ENTITY".equals(policy.getPolicyResourceCategory())) {
                List<String> entities = policy.getResources().get("entity").getValues();
                List<String> entityTypesRaw = policy.getResources().get("entity-type").getValues();

                if (entities.contains("*") && entityTypesRaw.contains("*")) {
                    Map<String, String> emptyMap = new HashMap<>();
                    shouldClauses.removeAll(shouldClauses);
                    shouldClauses.add(getMap("match_all",emptyMap));
                    break;
                }

                entities.remove("*");
                entityTypesRaw.remove("*");

                Set<String> entityTypes = new HashSet<>();
                entityTypesRaw.forEach(x -> entityTypes.addAll(getTypeAndSupertypesList(x)));

                if (!entities.isEmpty() && entityTypes.isEmpty()) {
                    combinedEntities.addAll(entities);
                } else if (entities.isEmpty() && !entityTypes.isEmpty()) {
                    combinedEntityTypes.addAll(entityTypes);
                } else if (!entities.isEmpty() && !entityTypes.isEmpty()) {
                    Map<String, Object> dslForPolicyResources = getDSLForResources(entities, entityTypes, null, null);
                    shouldClauses.add(dslForPolicyResources);
                }
            }
        }
        if (!combinedEntities.isEmpty()) {
            shouldClauses.add(getDSLForResources(combinedEntities, new HashSet<>(), null, null));
        }
        if (!combinedEntityTypes.isEmpty()) {
            shouldClauses.add(getDSLForResources(new ArrayList<>(), combinedEntityTypes, null, null));
        }
        return shouldClauses;
    }

    private static Map<String, Object> getDSLForResources(List<String> entities, Set<String> typeNames, List<String> classifications, String clauseName){
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

        List<Map<String, Object>> filterClauses = new ArrayList<>();

        if (!typeNames.isEmpty() && !typeNames.contains("*")) {
            filterClauses.add(getMap("terms", getMap("__typeName.keyword", typeNames)));
        }

        if (classifications != null && !classifications.isEmpty() && !classifications.contains("*")) {
            filterClauses.add(getMap("terms", getMap("__traitNames", classifications)));
            filterClauses.add(getMap("terms", getMap("__propagatedTraitNames", classifications)));
        }

        if (!filterClauses.isEmpty()) {
            boolClause.put("filter", filterClauses);
        }

        if (clauseName != null) {
            boolClause.put("_name", clauseName);
        }

        return getMap("bool", boolClause);
    }

    private static List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType) {
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
            policies = getFilteredPoliciesForUser(policies, user, groups, roles, policyType);
            policies = getFilteredPoliciesForActions(policies, actions, policyType);
        }
        return policies;

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

    private static List<RangerPolicy> getFilteredPoliciesForActions(List<RangerPolicy> policies, List<String> actions, String type) {
        List<RangerPolicy> filteredPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;
            if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
            if (policyItem != null) {
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

    private static List<RangerPolicy> getFilteredPoliciesForUser(List<RangerPolicy> policies, String user, List<String> groups, List<String> roles, String type) {
        List<RangerPolicy> filterPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;
            if (POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
            if (policyItem != null) {
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

    private static boolean listStartsWith(String value, List<String> list) {
        for (String item : list){
            if (item.startsWith(value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean listEndsWith(String value, List<String> list) {
        for (String item : list){
            if (item.endsWith(value)) {
                return true;
            }
        }
        return false;
    }

    public static String getCurrentUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null ? auth.getName() : "";
    }

    private static Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private static Set<String> getTypeAndSupertypesList(String typeName) {
        Set<String> entityTypes = typeRegistry.getEntityDefByName(typeName).getSuperTypes();
        entityTypes.add(typeName);

        return entityTypes;
    }*/

//    private List<AtlasEntityHeader> getPolicies() {
//        List<AtlasEntityHeader> ret = new ArrayList<>();
//        int from = 0;
//        int size = 1000;
//
//        IndexSearchParams indexSearchParams = new IndexSearchParams();
//        Map<String, Object> dsl = new HashMap<>();
//
//        List mustClauseList = new ArrayList();
//        mustClauseList.add(mapOf("term", getMap("__typeName.keyword", POLICY_ENTITY_TYPE)));
//        mustClauseList.add(mapOf("terms", getMap("policyServiceName", serviceNames)));
//        mustClauseList.add(mapOf("match", getMap("__state", ACTIVE_STATE_VALUE)));
//
//        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));
//        dsl.put("from", from);
//        dsl.put("size", size);
//
//        indexSearchParams.setDsl(dsl);
//        Set<String> attributes = new HashSet<>();
//        attributes.add("policyType");
//        attributes.add("policyServiceName");
//        attributes.add("policyCategory");
//        attributes.add("policySubCategory");
//        attributes.add("policyUsers");
//        attributes.add("policyGroups");
//        attributes.add("policyRoles");
//        attributes.add("policyActions");
//        attributes.add("policyResources");
//        attributes.add("policyResourceCategory");
//        attributes.add("policyFilterCriteria");
//        indexSearchParams.setAttributes(attributes);
//        indexSearchParams.setSuppressLogs(true);
//
//        AtlasSearchResult result = null;
//        try {
//            result = discoveryService.directIndexSearch(indexSearchParams);
//            lastUpdatedTime = System.currentTimeMillis();
//        } catch (AtlasBaseException e) {
//            LOG.error("Error getting policies!", e);
//        }
//        if (result != null) {
//            ret = result.getEntities();
//        }
//        return ret;
//    }

//    public void refreshPolicies() {
////        if (isPolicyUpdated()) {}
//        allPolicies = getPolicies();
//    }

//    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) throws JsonProcessingException {
//        List<Map<String, Object>> shouldClauses = new ArrayList<>();
//        for (RangerPolicy policy : policies) {
//            if ("RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
//                String filterCriteria = policy.getPolicyFilterCriteria();
//                ObjectMapper mapper = new ObjectMapper();
//                JsonNode filterCriteriaNode = mapper.readTree(filterCriteria);
//
//                JsonNode endOneFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
//                JsonNode endOneDsl = JsonToElasticsearchQuery.convertJsonToQuery(endOneFilterCriteriaNode, mapper);
//                String endOneDslBase64 = Base64.getEncoder().encodeToString(endOneDsl.toString().getBytes());
//                Map<String, Object> endOneFinalDsl = getMap("bool", getMap("filter", Arrays.asList(getMap("wrapper", getMap("query", endOneDslBase64)), endOneEntityDsl.toArray())));
//
//                JsonNode endTwoFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
//                JsonNode endTwoDsl = JsonToElasticsearchQuery.convertJsonToQuery(endTwoFilterCriteriaNode, mapper);
//                String endTwoDslBase64 = Base64.getEncoder().encodeToString(endTwoDsl.toString().getBytes());
//                Map<String, Object> endTwoFinalDsl = getMap("bool", getMap("filter", Arrays.asList(getMap("wrapper", getMap("query", endTwoDslBase64)), endTwoEntityDsl.toArray())));
//
//                Map<String, Object> policyDsl = new HashMap<>();
//                policyDsl.put("should", Arrays.asList(endOneFinalDsl, endTwoFinalDsl));
//                policyDsl.put("minimum_should_match", 1);
//                shouldClauses.add(getMap("bool", policyDsl));
//            }
//        }
//        return shouldClauses;
//    }
//
//    private static Map<String, Object> getDSLForRelationshipTagPolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) {
//        // To reduce the number of clauses
//        Set<String> allTags = new HashSet<>();
//        for (RangerPolicy policy : policies) {
//            if (!policy.getResources().isEmpty()) {
//                List<String> tags = policy.getResources().get("tag").getValues();
//                if (!tags.isEmpty()) {
//                    allTags.addAll(tags);
//                }
//            }
//        }
//        if (!allTags.isEmpty()) {
//            Map<String, Object> tagsDsl = getDSLForTags(allTags);
//            Map<String, Object> entityDsl = new HashMap<>();
//            entityDsl.put("should", Arrays.asList(endOneEntityDsl.toArray(), endTwoEntityDsl.toArray()));
//            entityDsl.put("minimum_should_match", 1);
//            return getMap("bool", getMap("filter", Arrays.asList(getDSLForTags(allTags), getMap("bool", entityDsl))));
//        }
//        return null;
//    }
//
//    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, List<Map<String, Object>> endOneEntityDsl, List<Map<String, Object>> endTwoEntityDsl) {
//        List<Map<String, Object>> shouldClauses = new ArrayList<>();
//        for (RangerPolicy policy : policies) {
//            if (!policy.getResources().isEmpty() && "RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
//                List<String> endOneEntities = policy.getResources().get("end-one-entity").getValues();
//                List<String> endOneEntityTypes = policy.getResources().get("end-one-entity-type").getValues();
//                List<String> endTwoEntities = policy.getResources().get("end-two-entity").getValues();
//                List<String> endTwoEntityTypes = policy.getResources().get("end-two-entity-type").getValues();
//
//                endOneEntities.remove("*");
//                endOneEntityTypes.remove("*");
//                endTwoEntities.remove("*");
//                endTwoEntityTypes.remove("*");
//
//                List<Map<String, Object>> endOneFilterList = new ArrayList<>();
//                if (!endOneEntities.isEmpty() || !endOneEntityTypes.isEmpty()) {
//                    Map<String, Object> endOneDsl = getDSLForResources(endOneEntities, endOneEntityTypes, null, null);
//                    endOneFilterList.add(endOneDsl);
//                }
//                endOneFilterList.addAll(endOneEntityDsl);
//                Map<String, Object> endOneFinalDsl = getMap("bool", getMap("filter", endOneFilterList));
//
//                List<Map<String, Object>> endTwoFilterList = new ArrayList<>();
//                if (!endTwoEntities.isEmpty() || !endTwoEntityTypes.isEmpty()) {
//                    Map<String, Object> endTwoDsl = getDSLForResources(endTwoEntities, endTwoEntityTypes, null, null);
//                    endOneFilterList.add(endTwoDsl);
//                }
//                endTwoFilterList.addAll(endTwoEntityDsl);
//                Map<String, Object> endTwoFinalDsl = getMap("bool", getMap("filter", endTwoFilterList));
//
//                Map<String, Object> policyDsl = new HashMap<>();
//                policyDsl.put("should", Arrays.asList(endOneFinalDsl, endTwoFinalDsl));
//                policyDsl.put("minimum_should_match", 1);
//                shouldClauses.add(getMap("bool", policyDsl));
//            }
//        }
//        return shouldClauses;
//    }

//    private List<String> getPolicyFilterCriteriaArray(List<AtlasEntityHeader> entityHeaders) {
//        AtlasPerfMetrics.MetricRecorder getPolicyFilterCriteriaArrayMetrics = RequestContext.get().startMetricRecord("getPolicyFilterCriteriaArray");
//        List<String> policyFilterCriteriaArray = new ArrayList<>();
//        if (entityHeaders != null) {
//            for (AtlasEntityHeader entity: entityHeaders) {
//                String policyFilterCriteria = (String) entity.getAttribute("policyFilterCriteria");
//                if (StringUtils.isNotEmpty(policyFilterCriteria)) {
//                    policyFilterCriteriaArray.add(policyFilterCriteria);
//                }
//            }
//        }
//        RequestContext.get().endMetricRecord(getPolicyFilterCriteriaArrayMetrics);
//        return policyFilterCriteriaArray;
//    }
//
//    private List<String> getPolicyDSLArray(List<String> policyFilterCriteriaArray) {
//        AtlasPerfMetrics.MetricRecorder getPolicyDSLArrayMetrics = RequestContext.get().startMetricRecord("getPolicyDSLArray");
//        List<String> policyDSLArray = new ArrayList<>();
//        ObjectMapper mapper = new ObjectMapper();
//        for (String policyFilterCriteria: policyFilterCriteriaArray) {
//            JsonNode policyFilterCriteriaNode = null;
//            try {
//                policyFilterCriteriaNode = mapper.readTree(policyFilterCriteria);
//            } catch (JsonProcessingException e) {
//                e.printStackTrace();
//            }
//            JsonNode policyDSL = JsonToElasticsearchQuery.convertJsonToQuery(policyFilterCriteriaNode, mapper);
//            policyDSLArray.add(policyDSL.toString());
//        }
//        RequestContext.get().endMetricRecord(getPolicyDSLArrayMetrics);
//        return policyDSLArray;
//    }

//    private Map<String, Object> getDSLForApePolicies(List<AtlasEntityHeader> policies){
//        if (policies!=null && !policies.isEmpty()) {
//            List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
//            List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
//            List<Map<String, Object>> shouldClauseList = new ArrayList<>();
//            for (String policyDSL: policyDSLArray) {
//                String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
//                shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
//            }
//            Map<String, Object> boolClause = new HashMap<>();
//            boolClause.put("should", shouldClauseList);
//            boolClause.put("minimum_should_match", 1);
//            return getMap("bool", boolClause);
//        }
//        return null;
//    }
//
//    private List<String> getEntitiesFromResources(List<String> policyResources, String policyCategory, String policySubCategory){
//        List<String> entities = new ArrayList<>();
//        for(String resource: policyResources) {
//            if (resource.contains("{USER}")) {
//                resource.replace("{USER}", RequestContext.getCurrentUser());
//            }
//            if (resource.startsWith("entity:")) {
//                if (!resource.substring(7).equals("*")) {
//                    entities.add(resource.substring(7));
//                    if (policyCategory.equals("persona")) {
//                        if (policySubCategory.equals("metadata")) {
//                            entities.add(resource.substring(7) + "/*");
//                        } else if (policySubCategory.equals("glossary")) {
//                            entities.add("*@" + resource.substring(7));
//                        }
//                    }
//                }
//            }
//        }
//        return entities;
//    }
//
//    private List<String> getEntityTypesFromResources(List<String> policyResources){
//        List<String> entityTypes = new ArrayList<>();
//        for(String resource: policyResources) {
//            if (resource.startsWith("entity-type:") && !resource.equals("entity-type:*")) {
//                entityTypes.add(resource.substring(12));
//            }
//        }
//        return entityTypes;
//    }
//
//    private List<String> getTagsFromResources(List<String> policyResources){
//        List<String> tags = new ArrayList<>();
//        for(String resource: policyResources) {
//            if (resource.startsWith("tag:") && !resource.equals("tag:*")) {
//                tags.add(resource.substring(4));
//            }
//        }
//        return tags;
//    }
//
//    private Map<String, Object> getDSLForEntityResources(List<String> entities){
//        if (!entities.isEmpty()) {
//            List<Map<String, Object>> shouldClauses = new ArrayList<>();
//            List<String> termsQualifiedNames = new ArrayList<>();
//            for (String entity: entities) {
//                if (entity.contains("*")) {
//                    shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
//                } else {
//                    termsQualifiedNames.add(entity);
//                }
//            }
//            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
//
//            Map<String, Object> boolClause = new HashMap<>();
//            boolClause.put("should", shouldClauses);
//            boolClause.put("minimum_should_match", 1);
//            return getMap("bool", boolClause);
//        }
//        return null;
//    }
//
//    private Map<String, Object> getDSLForEntityTypeResources(List<String> typeNames){
//        if (!typeNames.isEmpty()) {
//            return getMap("terms", getMap("__typeName.keyword", typeNames));
//        }
//        return null;
//    }
//
//    private Map<String, Object> getDSLForPolicyResources(List<String> entities, List<String> typeNames){
//        List<Map<String, Object>> shouldClauses = new ArrayList<>();
//        List<String> termsQualifiedNames = new ArrayList<>();
//        for (String entity: entities) {
//            if (entity.contains("*")) {
//                shouldClauses.add(getMap("wildcard", getMap("qualifiedName", entity)));
//            } else {
//                termsQualifiedNames.add(entity);
//            }
//        }
//        if (!termsQualifiedNames.isEmpty()) {
//            shouldClauses.add(getMap("terms", getMap("qualifiedName", termsQualifiedNames)));
//        }
//
//        Map<String, Object> boolClause = new HashMap<>();
//        boolClause.put("should", shouldClauses);
//        boolClause.put("minimum_should_match", 1);
//        boolClause.put("filter", getMap("terms", getMap("__typeName.keyword", typeNames)));
//
//        return getMap("bool", boolClause);
//    }
//
//    private Map<String, Object> getDSLForTagResources(Set<String> tags){
//        List<Map<String, Object>> shouldClauses = new ArrayList<>();
//        shouldClauses.add(getMap("terms", getMap("__traitNames", tags)));
//        shouldClauses.add(getMap("terms", getMap("__propagatedTraitNames", tags)));
//
//        Map<String, Object> boolClause = new HashMap<>();
//        boolClause.put("should", shouldClauses);
//        boolClause.put("minimum_should_match", 1);
//
//        return getMap("bool", boolClause);
//    }

//    public static void verifyAccess(AtlasEntity entity, String action) throws AtlasBaseException {
//        try {
//            if (!isCreateAccessAllowed(entity, action)) {
//                throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, RequestContext.getCurrentUser(), "Unauthorised");
//            }
//        } catch (AtlasBaseException e) {
//            throw e;
//        }
//    }
//
//    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies,String endOneGuid, String endTwoGuid) throws JsonProcessingException {
//        return getDSLForRelationshipAbacPolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
//    }
//
//    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) throws JsonProcessingException {
//        return getDSLForRelationshipAbacPolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
//    }
//
//    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, String endOneGuid, String endTwoGuid) {
//        return getDSLForRelationshipResourcePolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
//    }
//
//    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) {
//        return getDSLForRelationshipResourcePolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
//    }
//
//    public Map<String, Object> getIndexsearchPreFilterDSL(String persona, String purpose) {
//        RangerUserStore userStore = usersGroupsRolesStore.getUserStore();
//        RangerRoles allRoles = usersGroupsRolesStore.getAllRoles();
//
//        String user = getCurrentUserName();
//        Map<String, Set<String>> userGroupMapping = userStore.getUserGroupMapping();
//        List<String> groups = new ArrayList<>();
//        Set<String> groupsSet = userGroupMapping.get(user);
//        if (groupsSet != null && !groupsSet.isEmpty()) {
//            groups.addAll(groupsSet);
//        }
//
//        List<String> roles = new ArrayList<>();
//        Set<RangerRole> rangerRoles = allRoles.getRangerRoles();
//        for (RangerRole role : rangerRoles) {
//            List<RangerRole.RoleMember> users = role.getUsers();
//            for (RangerRole.RoleMember roleUser: users) {
//                if (roleUser.getName().equals(user)) {
//                    roles.add(role.getName());
//                }
//            }
//        }
//
//        List<String> actions = new ArrayList<>();
//        actions.add("entity-read");
//        actions.add("persona-asset-read");
//        actions.add("domain-entity-read");
//
//        List<Map<String, Object>> shouldList = new ArrayList<>();
//        List<AtlasEntityHeader> apePolicies = new ArrayList<>();
//
//        List<String> combinedEntities = new ArrayList<>();
//        List<String> combinedEntityTypes = new ArrayList<>();
//        Set<String> combinedTags = new HashSet<>();
//
//        if (allPolicies == null) {
//            allPolicies = getPolicies();
//        }
//
//        for (AtlasEntityHeader policy : allPolicies) {
//            List<String> policyUsers = (List<String>) policy.getAttribute("policyUsers");
//            List<String> policyGroups = (List<String>) policy.getAttribute("policyGroups");
//            List<String> policyRoles = (List<String>) policy.getAttribute("policyRoles");
//            List<String> policyActions = (List<String>) policy.getAttribute("policyActions");
//            List<String> policyResources = (List<String>) policy.getAttribute("policyResources");
//            String policyCategory = (String) policy.getAttribute("policyCategory");
//            String policySubCategory = (String) policy.getAttribute("policySubCategory");
//            String policyServiceName = (String) policy.getAttribute("policyServiceName");
//            String policyResourceCategory = (String) policy.getAttribute("policyResourceCategory");
//            String policyType = (String) policy.getAttribute("policyType");
//            String policyQualifiedName = (String) policy.getAttribute("qualifiedName");
//
//            if (persona != null && !policyQualifiedName.startsWith(persona)) {
//                continue;
//            } else if (purpose != null && !policyQualifiedName.startsWith(purpose)) {
//                continue;
//            }
//            if (policyType != null && policyType.equals("allow")) {
//                if (policyUsers.contains(user) || arrayListContains(policyGroups, groups) || arrayListContains(policyRoles, roles)) {
//                    if (arrayListContains(policyActions, actions)) {
//                        if (policyServiceName.equals("atlas_tag")) {
//                            List<String> policyTags = getTagsFromResources(policyResources);
//                            if (!policyTags.isEmpty()) {
//                                combinedTags.addAll(policyTags);
//                            }
//                        } else if (policyServiceName.equals("ape")) {
//                            apePolicies.add(policy);
//                        }
//                        else if (policyServiceName.equals("atlas") && (policyResourceCategory.equals("ENTITY") || policyResourceCategory.equals("CUSTOM"))) {
//                            List<String> policyEntities = getEntitiesFromResources(policyResources, policyCategory, policySubCategory);
//                            List<String> policyEntityTypes = getEntityTypesFromResources(policyResources);
//                            if (!policyEntities.isEmpty() && policyEntityTypes.isEmpty()) {
//                                combinedEntities.addAll(policyEntities);
//                            } else if (policyEntities.isEmpty() && !policyEntityTypes.isEmpty()) {
//                                combinedEntityTypes.addAll(policyEntityTypes);
//                            } else if (!policyEntities.isEmpty() && !policyEntityTypes.isEmpty()) {
//                                Map<String, Object> dslForPolicyResources = getDSLForPolicyResources(policyEntities, policyEntityTypes);
//                                shouldList.add(dslForPolicyResources);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        if (!combinedTags.isEmpty()) {
//            Map<String, Object> dslForPolicyResources = getDSLForTagResources(combinedTags);
//            shouldList.add(dslForPolicyResources);
//        }
//        if (!combinedEntities.isEmpty()) {
//            Map<String, Object> DSLForEntityResources = getDSLForEntityResources(combinedEntities);
//            if (DSLForEntityResources != null) {
//                shouldList.add(DSLForEntityResources);
//            }
//        }
//        if (!combinedEntityTypes.isEmpty()) {
//            Map<String, Object> DSLForEntityTypeResources = getDSLForEntityTypeResources(combinedEntityTypes);
//            if (DSLForEntityTypeResources != null) {
//                shouldList.add(DSLForEntityTypeResources);
//            }
//        }
//        if (!apePolicies.isEmpty()) {
//            Map<String, Object> DSLForApePolicies = getDSLForApePolicies(apePolicies);
//            if (DSLForApePolicies != null) {
//                shouldList.add(DSLForApePolicies);
//            }
//        }
//
//        Map<String, Object> allPreFiltersBoolClause = new HashMap<>();
//        allPreFiltersBoolClause.put("should", shouldList);
//        allPreFiltersBoolClause.put("minimum_should_match", 1);
//        return allPreFiltersBoolClause;
//    }
//
//    private static Map<String, Object> getDSLForTagResourcePolicies(List<RangerPolicy> policies, String endOneGuid, String endTwoGuid) {
//        return getDSLForRelationshipTagPolicies(policies, Arrays.asList(getMap("term", getMap("__guid", endOneGuid))), Arrays.asList(getMap("term", getMap("__guid", endTwoGuid))));
//    }
//
//    private static Map<String, Object> getDSLForTagResourcePolicies(List<RangerPolicy> policies, String endOneQualifiedName, String endOneTypeName, String endTwoQualifiedName, String endTwoTypeName) {
//        return getDSLForRelationshipTagPolicies(policies, Arrays.asList(getMap("term", getMap("__typeName.keyword", endOneTypeName)), getMap("term", getMap("qualifiedName", endOneQualifiedName))), Arrays.asList(getMap("term", getMap("__typeName.keyword", endTwoTypeName)), getMap("term", getMap("qualifiedName", endTwoQualifiedName))));
//    }

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

}
