package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.authorizer.AuthorizerCommon.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.AuthorizerCommon.arrayListContains;
import static org.apache.atlas.authorizer.AuthorizerCommon.getMap;
import static org.apache.atlas.authorizer.ListAuthorizer.getDSLForResources;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

public class RelationshipAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAuthorizer.class);

    public static boolean isAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        boolean deny = checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, AuthorizerCommon.POLICY_TYPE_DENY);
        if (deny) {
            return false;
        }
        return checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, POLICY_TYPE_ALLOW);
    }

    public static boolean checkRelationshipAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity,
                                                         AtlasEntityHeader endTwoEntity, String policyType) throws AtlasBaseException {
        //Relationship add, update, remove access check in memory
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("checkRelationshipAccessAllowedInMemory."+policyType);

        try {
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
                if (filterCriteriaNode != null && filterCriteriaNode.get("endOneEntity") != null) {
                    JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
                    eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(endOneEntity));

                    if (eval) {
                        entityFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
                        eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(endTwoEntity));
                    }
                }
                ret = ret || eval;
                if (ret) {
                    break;
                }
            }

            if (!ret) {
                List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action), policyType);
                List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas", Collections.singletonList(action), policyType);

                tagPolicies.addAll(resourcePolicies);

                ret = validateResourcesForCreateRelationship(tagPolicies, relationshipType, endOneEntity, endTwoEntity);
            }

            return ret;
        } catch (NullPointerException e) {
            return false;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private static boolean validateResourcesForCreateRelationship(List<RangerPolicy> resourcePolicies, String relationshipType,
                                                                  AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateResourcesForCreateRelationship");

        RangerPolicy matchedPolicy = null;

        Set<String> endOneEntityTypes = AuthorizerCommon.getTypeAndSupertypesList(endOneEntity.getTypeName());
        Set<String> endTwoEntityTypes = AuthorizerCommon.getTypeAndSupertypesList(endTwoEntity.getTypeName());

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

                    if ("relationship-type".equals(resource)) {
                        if (!values.contains(("*"))) {
                            Optional<String> match = values.stream().filter(x -> relationshipType.matches(x
                                            .replace("*", ".*")))
                                    .findFirst();

                            if (!match.isPresent()) {
                                resourcesMatched = false;
                                break;
                            }
                        }
                    }

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
                                            .replace("{USER}", AuthorizerCommon.getCurrentUserName())
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

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    public static boolean validateFilterCriteriaWithEntity(JsonNode data, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.validateFilterCriteriaWithEntity");

        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        Set<String> assetTypes = AuthorizerCommon.getTypeAndSupertypesList(entity.getTypeName());

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
                    attributeName = attributeName.replace(".text", "");
                } else if (attributeName.endsWith(".keyword")) {
                    attributeName = attributeName.replace(".keyword", "");
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
                    AtlasEntityType entityType = AuthorizerCommon.getEntityTypeByName(typeName);
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
                if ((operator.equals("STARTS_WITH") && AuthorizerCommon.listStartsWith(attributeValue, entityAttributeValues))) {
                    evaluation = true;
                }
                if ((operator.equals("ENDS_WITH") && AuthorizerCommon.listEndsWith(attributeValue, entityAttributeValues))) {
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

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    public static boolean isRelationshipAccessAllowed(String action, String endOneGuid, String endTwoGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.isRelationshipAccessAllowed");

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
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return false;
    }

    public static Map<String, Object> getElasticsearchDSLForRelationshipActions(List<String> actions, String endOneGuid, String endTwoGuid) throws JsonProcessingException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.getElasticsearchDSLForRelationshipActions");

        List<Map<String, Object>> policiesClauses = new ArrayList<>();
        List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas", actions, POLICY_TYPE_ALLOW);
        List<Map<String, Object>> resourcePoliciesClauses = getDSLForRelationshipResourcePolicies(resourcePolicies);

        List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_tag", actions, POLICY_TYPE_ALLOW);
        List<Map<String, Object>> tagPoliciesClauses = getDSLForRelationshipTagPolicies(tagPolicies);

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", actions, POLICY_TYPE_ALLOW);
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


        RequestContext.get().endMetricRecord(recorder);
        return getMap("query", getMap("bool", boolClause));
    }

    private static List<Map<String, Object>> getDSLForRelationshipResourcePolicies(List<RangerPolicy> policies) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.getDSLForRelationshipResourcePolicies");

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

                    //Set<String> entityTypes = new HashSet<>();
                    //entityTypesRaw.forEach(x -> entityTypes.addAll(getTypeAndSupertypesList(x)));

                    List<String> entityClassifications = policy.getResources().get(entityClassificationParamName).getValues();
                    if (entities.contains("*") && entityTypesRaw.contains("*") && entityClassifications.contains("*")) {
                        shouldClauses.add(getMap("match_all", getMap("_name", clauseName)));
                    } else {
                        Map<String, Object> dslForPolicyResources = getDSLForResources(entities, new HashSet<>(entityTypesRaw), entityClassifications, clauseName);
                        shouldClauses.add(dslForPolicyResources);
                    }
                }
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return shouldClauses;
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
                    JsonNode endFilterCriteriaNode = filterCriteriaNode.get(relationshipEnd.equals("end-one")  ? "endOneEntity" : "endTwoEntity");
                    JsonNode Dsl = JsonToElasticsearchQuery.convertJsonToQuery(endFilterCriteriaNode, mapper);
                    String DslBase64 = Base64.getEncoder().encodeToString(Dsl.toString().getBytes());
                    String clauseName = relationshipEnd + "-" + policy.getGuid();
                    Map<String, Object> boolMap = new HashMap<>();
                    boolMap.put("_name", clauseName);
                    boolMap.put("filter", getMap("wrapper", getMap("query", DslBase64)));

                    shouldClauses.add(getMap("bool", boolMap));
                }
            }
        }
        return shouldClauses;
    }
}
