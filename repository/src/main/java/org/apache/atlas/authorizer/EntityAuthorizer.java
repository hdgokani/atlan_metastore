package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.glossary.relations.AtlasTermAssignmentHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

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
                break;
            }
        }

        if (!ret) {
            List<RangerPolicy> tagPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_tag", Collections.singletonList(action), policyType);
            List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas", Collections.singletonList(action), policyType);

            tagPolicies.addAll(resourcePolicies);

            ret = validateResourcesForCreateEntityInMemory(tagPolicies, entity);
        }

        return ret;
    }

    private static boolean validateResourcesForCreateEntityInMemory(List<RangerPolicy> resourcePolicies, AtlasEntity entity) {
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

        return false;
    }

    public static boolean validateFilterCriteriaWithEntity(JsonNode data, AtlasEntity entity) {
        AtlasPerfMetrics.MetricRecorder convertJsonToQueryMetrics = RequestContext.get().startMetricRecord("convertJsonToQuery");
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

        RequestContext.get().endMetricRecord(convertJsonToQueryMetrics);
        return result;
    }
}
