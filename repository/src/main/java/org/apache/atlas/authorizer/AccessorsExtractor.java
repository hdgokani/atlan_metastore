package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAccessorRequest;
import org.apache.atlas.authorize.AtlasAccessorResponse;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.authorizers.AuthorizerCommon;
import org.apache.atlas.authorizer.authorizers.EntityAuthorizer;
import org.apache.atlas.authorizer.authorizers.ListAuthorizer;
import org.apache.atlas.authorizer.authorizers.RelationshipAuthorizer;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorize.AtlasPrivilege.*;
import static org.apache.atlas.authorizer.AuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.authorizers.EntityAuthorizer.validateFilterCriteriaWithEntity;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class AccessorsExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(AccessorsExtractor.class);

    private static final Set<String> ENTITY_ACTIONS = new HashSet<String>(){{
        add(ENTITY_READ.getType());
        add(ENTITY_CREATE.getType());
        add(ENTITY_UPDATE.getType());
        add(ENTITY_DELETE.getType());
        add(ENTITY_READ_CLASSIFICATION.getType());
        add(ENTITY_ADD_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_CLASSIFICATION.getType());
        add(ENTITY_REMOVE_CLASSIFICATION.getType());
        add(ENTITY_UPDATE_BUSINESS_METADATA.getType());
        add(ENTITY_ADD_LABEL.getType());
        add(ENTITY_REMOVE_LABEL.getType());
    }};

    private static final Set<String> RELATIONSHIP_ACTIONS = new HashSet<String>(){{
        add(RELATIONSHIP_ADD.getType());
        add(RELATIONSHIP_UPDATE.getType());
        add(RELATIONSHIP_REMOVE.getType());
    }};

    public static AtlasAccessorResponse getAccessors(AtlasAccessorRequest request) throws AtlasBaseException {
        return getAccessorsInMemory(request);
    }

    private static void collectSubjects(AtlasAccessorResponse response, List<RangerPolicy> matchedPolicies) {
        for (RangerPolicy policy: matchedPolicies) {
            List<RangerPolicy.RangerPolicyItem> policyItems = null;
            if (CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
                for (RangerPolicy.RangerPolicyItem policyItem:  policy.getPolicyItems()) {
                    response.getUsers().addAll(policyItem.getUsers());
                    response.getRoles().addAll(policyItem.getRoles());
                    response.getGroups().addAll(policyItem.getGroups());
                }
            } else if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
                for (RangerPolicy.RangerPolicyItem policyItem:  policy.getDenyPolicyItems()) {
                    response.getDenyUsers().addAll(policyItem.getUsers());
                    response.getDenyRoles().addAll(policyItem.getRoles());
                    response.getDenyGroups().addAll(policyItem.getGroups());
                }
            }
        }
    }

    private static AtlasEntity extractEntity(String guid, String qualifiedName, String typeName) throws AtlasBaseException {
        AtlasEntity entity = null;

        if (StringUtils.isNotEmpty(guid)) {
            entity = AuthorizerCommon.toAtlasEntityHeaderWithClassifications(guid);

        } else {
            AtlasEntityType entityType = AuthorizerCommon.getEntityTypeByName(typeName);
            if (entityType != null) {
                try {
                    AtlasVertex vertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(typeName, QUALIFIED_NAME, qualifiedName);
                    if (vertex != null) {
                        entity = AuthorizerCommon.toAtlasEntityHeaderWithClassifications(vertex);
                    } else {
                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put(QUALIFIED_NAME, qualifiedName);
                        entity = new AtlasEntity(entityType.getTypeName(), attributes);
                    }

                } catch (AtlasBaseException abe) {
                    if (abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                        throw abe;
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put(QUALIFIED_NAME, qualifiedName);
                    entity = new AtlasEntity(entityType.getTypeName(), attributes);
                }
            } else {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(QUALIFIED_NAME, qualifiedName);
                entity = new AtlasEntity(typeName, attributes);
            }
        }
        return entity;
    }

    public static AtlasAccessorResponse getAccessorsInMemory(AtlasAccessorRequest request) throws AtlasBaseException {
        AtlasAccessorResponse response = new AtlasAccessorResponse();

        String action = AtlasPrivilege.valueOf(request.getAction()).getType();

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), null, true);

        List<RangerPolicy> matchedPolicies = getAccessorsInMemoryForAbacPolicies(request, abacPolicies);


        List<RangerPolicy> resourcePolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas", Arrays.asList(action), null, true);
        resourcePolicies.addAll(PoliciesStore.getRelevantPolicies(null, null, "atlas_tag", Arrays.asList(action), null, true));

        matchedPolicies.addAll(getAccessorsInMemoryForRangerPolicies(request, resourcePolicies));


        collectSubjects(response, matchedPolicies);

        return response;
    }

    public static List<RangerPolicy> getAccessorsInMemoryForRangerPolicies(AtlasAccessorRequest request, List<RangerPolicy> rangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> matchedPolicies = new ArrayList<>();
        String action = AtlasPrivilege.valueOf(request.getAction()).getType();

        if (ENTITY_ACTIONS.contains(action)) {
            AtlasEntity entity = extractEntity(request.getGuid(), request.getQualifiedName(), request.getTypeName());
            Set<String> entityTypes = AuthorizerCommon.getTypeAndSupertypesList(request.getTypeName());

            boolean matched = false;
            for (RangerPolicy policy: rangerPolicies) {
                matched = EntityAuthorizer.evaluateRangerPolicyInMemory(policy, entity, entityTypes);

                if (matched) {
                    matchedPolicies.add(policy);
                }
            }

        } else if (RELATIONSHIP_ACTIONS.contains(action)) {
            AtlasEntityHeader entityOne = new AtlasEntityHeader(extractEntity(request.getEntityGuidEnd1(), request.getEntityQualifiedNameEnd1(), request.getEntityTypeEnd1()));
            AtlasEntityHeader entityTwo = new AtlasEntityHeader(extractEntity(request.getEntityGuidEnd2(), request.getEntityQualifiedNameEnd2(), request.getEntityTypeEnd2()));

            Set<String> entityTypesOne = AuthorizerCommon.getTypeAndSupertypesList(request.getEntityTypeEnd1());
            Set<String> entityTypesTwo = AuthorizerCommon.getTypeAndSupertypesList(request.getEntityTypeEnd2());

            boolean matched = false;
            for (RangerPolicy policy: rangerPolicies) {
                matched = RelationshipAuthorizer.evaluateRangerPolicyInMemory(policy, request.getRelationshipTypeName(), entityOne, entityTwo, entityTypesOne, entityTypesTwo);

                if (matched) {
                    matchedPolicies.add(policy);
                }
            }
        }

        return matchedPolicies;
    }

    public static List<RangerPolicy> getAccessorsInMemoryForAbacPolicies(AtlasAccessorRequest request, List<RangerPolicy> abacPolicies) throws AtlasBaseException {
        List<RangerPolicy> matchedPolicies = new ArrayList<>();
        String action = AtlasPrivilege.valueOf(request.getAction()).getType();

        ObjectMapper mapper = new ObjectMapper();

        if (ENTITY_ACTIONS.contains(action)) {
            AtlasEntity entity = extractEntity(request.getGuid(), request.getQualifiedName(), request.getTypeName());
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());
            if (vertex == null) {
                vertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(request.getTypeName(), QUALIFIED_NAME, request.getQualifiedName());
            }

            for (RangerPolicy policy : abacPolicies) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                if (filterCriteria != null && !filterCriteria.isEmpty() ) {

                    JsonNode filterCriteriaNode = null;
                    try {
                        filterCriteriaNode = mapper.readTree(policy.getPolicyFilterCriteria());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                        JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                        boolean matched = EntityAuthorizer.validateFilterCriteriaWithEntity(entityFilterCriteriaNode, entity, vertex);

                        if (matched) {
                            matchedPolicies.add(policy);
                        }
                    }
                }
            }

        } else if (RELATIONSHIP_ACTIONS.contains(action)) {
            AtlasEntityHeader entityOne = new AtlasEntityHeader(extractEntity(request.getEntityGuidEnd1(), request.getEntityQualifiedNameEnd1(), request.getEntityTypeEnd1()));
            AtlasEntityHeader entityTwo = new AtlasEntityHeader(extractEntity(request.getEntityGuidEnd2(), request.getEntityQualifiedNameEnd2(), request.getEntityTypeEnd2()));

            AtlasVertex vertexOne = AtlasGraphUtilsV2.findByGuid(request.getEntityGuidEnd1());
            AtlasVertex vertexTwo = AtlasGraphUtilsV2.findByGuid(request.getEntityGuidEnd2());
            if (vertexOne == null) {
                vertexOne = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(request.getEntityTypeEnd1(), QUALIFIED_NAME, request.getEntityQualifiedNameEnd1());
            }
            if (vertexTwo == null) {
                vertexTwo = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(request.getEntityTypeEnd2(), QUALIFIED_NAME, request.getEntityQualifiedNameEnd2());
            }

            for (RangerPolicy policy : abacPolicies) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                if (filterCriteria != null && !filterCriteria.isEmpty() ) {

                    JsonNode filterCriteriaNode = null;
                    try {
                        filterCriteriaNode = mapper.readTree(policy.getPolicyFilterCriteria());
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (filterCriteriaNode != null && filterCriteriaNode.get("endOneEntity") != null) {
                        JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
                        boolean matched = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(entityOne), vertexOne);

                        if (matched) {
                            entityFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
                            matched = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, new AtlasEntity(entityTwo), vertexTwo);
                        }

                        if (matched) {
                            matchedPolicies.add(policy);
                        }
                    }
                }
            }
        }

        return  matchedPolicies;
    }
}
