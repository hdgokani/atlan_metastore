package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.authorizer.AuthorizerCommon.getMap;
import static org.apache.atlas.authorizer.ListAuthorizer.getDSLForAbacPolicies;
import static org.apache.atlas.authorizer.ListAuthorizer.getDSLForResourcePolicies;
import static org.apache.atlas.authorizer.ListAuthorizer.getDSLForTagPolicies;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

public class EntityAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorizationUtils.class);

    private static final String POLICY_TYPE_ALLOW = "allow";
    private static final String POLICY_TYPE_DENY = "deny";

    public static boolean isAccessAllowedInMemory(AtlasEntity entity, String action) {
        boolean deny = isAccessAllowedInMemory(entity, action, POLICY_TYPE_DENY);
        if (deny) {
            return false;
        }
        return isAccessAllowedInMemory(entity, action, POLICY_TYPE_ALLOW);
    }

    public static boolean isAccessAllowedInMemory(AtlasEntity entity, String action, String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("isAccessAllowedInMemory."+policyType);
        List<RangerPolicy> policies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
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
                LOG.info("Matched with criteria {} : {}", policyType, filterCriteria);
                break;
            }
        }

        if (!ret) {
            List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action), policyType);
            List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas", Collections.singletonList(action), policyType);

            tagPolicies.addAll(resourcePolicies);

            ret = validateResourcesForCreateEntityInMemory(tagPolicies, entity);
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    private static boolean validateResourcesForCreateEntityInMemory(List<RangerPolicy> resourcePolicies, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateResourcesForCreateEntityInMemory");
        RangerPolicy matchedPolicy = null;
        Set<String> entityTypes = AuthorizerCommon.getTypeAndSupertypesList(entity.getTypeName());

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
                        if (!values.contains(("*"))) {
                            String assetQualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
                            Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x
                                            .replace("{USER}", AuthorizerCommon.getCurrentUserName())
                                            .replace("*", ".*")))
                                    .findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    if ("entity-business-metadata".equals(resource)) {
                        if (!values.contains(("*"))) {
                            String assetQualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
                            Optional<String> match = values.stream().filter(x -> assetQualifiedName.matches(x
                                            .replace("{USER}", AuthorizerCommon.getCurrentUserName())
                                            .replace("*", ".*")))
                                    .findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

                    //for tag based policy
                    if ("tag".equals(resource)) {
                        if (!values.contains(("*"))) {
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
                }

                if (resourcesMatched) {
                    matchedPolicy = rangerPolicy;
                    LOG.info("Matched with policy: {}:{}", matchedPolicy.getName(), matchedPolicy.getGuid());
                    return true;
                }
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    public static boolean validateFilterCriteriaWithEntity(JsonNode data, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateFilterCriteriaWithEntity");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        if (criterion.size() == 0) {
            return false;
        }
        boolean result = true;

        Set<String> assetTypes = AuthorizerCommon.getTypeAndSupertypesList(entity.getTypeName());

        for (JsonNode crit : criterion) {

            result = true;
            if (condition.equals("OR")) {
                result = false;
            }
            boolean evaluation = false;

            if (crit.has("condition")) {
                evaluation = validateFilterCriteriaWithEntity(crit, entity);
            } else {
                evaluation = evaluateFilterCriteria(crit, entity, assetTypes);
            }

            if (condition.equals("AND")) {
                if (!evaluation) {
                    return false;
                }
                result = true;
            } else {
                result = result || evaluation;
                break;
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static boolean evaluateFilterCriteria(JsonNode crit, AtlasEntity entity, Set<String> assetTypes) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("evaluateFilterCriteria");
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
            attributeName = attributeName.replace(".text", "");
        } else if (attributeName.endsWith(".keyword")) {
            attributeName = attributeName.replace(".keyword", "");
        }

        List<String> entityAttributeValues = new ArrayList<>();

        switch (attributeName) {
            case "__superTypeNames":
                entityAttributeValues.addAll(assetTypes);

                break;
            case "__typeName":
                entityAttributeValues.add(entity.getTypeName());

                break;
            case "__guid":
                entityAttributeValues.add(entity.getGuid());

                break;
            case "__traitNames":
                List<AtlasClassification> atlasClassifications = entity.getClassifications();
                if (atlasClassifications != null && !atlasClassifications.isEmpty()) {
                    for (AtlasClassification atlasClassification : atlasClassifications) {
                        entityAttributeValues.add(atlasClassification.getTypeName());
                    }
                }
                break;
            case "__meanings":
                List<AtlasObjectId> atlasMeanings = (List<AtlasObjectId>) entity.getRelationshipAttribute("meanings");
                if (CollectionUtils.isNotEmpty(atlasMeanings)) {
                    for (AtlasObjectId atlasMeaning : atlasMeanings) {
                        entityAttributeValues.add((String) atlasMeaning.getUniqueAttributes().get(QUALIFIED_NAME));
                    }
                }
                break;
            /*case "__meaningNames":
                atlasMeanings = entity.getMeanings();
                for (AtlasTermAssignmentHeader atlasMeaning : atlasMeanings) {
                    entityAttributeValues.add(atlasMeaning.getDisplayText());
                }
                break;*/
            default:
                String typeName = entity.getTypeName();
                boolean isArrayOfPrimitiveType = false;
                boolean isArrayOfEnum = false;
                AtlasEntityType entityType = AuthorizerCommon.getEntityTypeByName(typeName);
                AtlasStructType.AtlasAttribute atlasAttribute = entityType.getAttribute(attributeName);
                if (atlasAttribute != null && atlasAttribute.getAttributeType().getTypeCategory().equals(ARRAY)) {
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
                break;
        }

        switch (operator) {
            case "EQUALS":
                if (entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;
            case "STARTS_WITH":
                if (AuthorizerCommon.listStartsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "LIKE":
                if (AuthorizerCommon.listMatchesWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "ENDS_WITH":
                if (AuthorizerCommon.listEndsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "NOT_EQUALS":
                if (!entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;

            default: LOG.warn("Found unknown operator {}", operator);
        }

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    public static boolean isAccessAllowed(String guid, String action) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.isAccessAllowed");
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

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    public static boolean isAccessAllowedEvaluator(String entityTypeName, String entityQualifiedName, String action) throws AtlasBaseException {
        List<Map<String, Object>> filterClauseList = new ArrayList<>();
        Map<String, Object> policiesDSL = getElasticsearchDSL(null, null, Arrays.asList(action));
        filterClauseList.add(policiesDSL);
        filterClauseList.add(getMap("wildcard", getMap("__typeName.keyword", entityTypeName)));
        if (entityQualifiedName != null)
            filterClauseList.add(getMap("wildcard", getMap("qualifiedName", entityQualifiedName)));
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

    public static Map<String, Object> getElasticsearchDSL(String persona, String purpose, List<String> actions) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.getElasticsearchDSL");
        Map<String, Object> allowDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_ALLOW);
        Map<String, Object> denyDsl = getElasticsearchDSLForPolicyType(persona, purpose, actions, POLICY_TYPE_DENY);
        Map<String, Object> finaDsl = new HashMap<>();
        if (allowDsl != null) {
            finaDsl.put("filter", allowDsl);
        }
        if (denyDsl != null) {
            finaDsl.put("must_not", denyDsl);
        }
        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", finaDsl);
    }

    private static Integer getCountFromElasticsearch(String query) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.getCountFromElasticsearch");
        RestClient restClient = getLowLevelClient();
        AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
        Map<String, Object> elasticsearchResult = null;
        elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(query);
        Integer count = null;
        if (elasticsearchResult!=null) {
            count = (Integer) elasticsearchResult.get("total");
        }
        RequestContext.get().endMetricRecord(recorder);
        return count;
    }

    public static Map<String, Object> getElasticsearchDSLForPolicyType(String persona, String purpose, List<String> actions, String policyType) {
        List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas", actions, policyType);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas_tag", actions, policyType);
        Map<String, Object> tagPoliciesClause = getDSLForTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(persona, purpose, "atlas_abac", actions, policyType);
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
}
