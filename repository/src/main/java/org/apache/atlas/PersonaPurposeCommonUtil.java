package org.apache.atlas;

import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class PersonaPurposeCommonUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaPurposeCommonUtil.class);

    public static final String RESOURCE_PREFIX = "resource:";

    public static final String POLICY_TYPE_ACCESS = "0";
    public static final String POLICY_TYPE_DATAMASK = "1";

    public static final String ACCESS_ENTITY_READ = "entity-read";
    public static final String ACCESS_ADD_REL     = "add-relationship";
    public static final String ACCESS_UPDATE_REL  = "update-relationship";
    public static final String ACCESS_REMOVE_REL  = "remove-relationship";

    public static final String LINK_ASSET_ACTION = "link-assets";

    public static String getUUID() {
        return NanoIdUtils.randomNanoId(22);
    }

    public static String getName(AtlasEntity entity) {
        return (String) entity.getAttribute(NAME);
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return (String) entity.getAttribute(QUALIFIED_NAME);
    }

    public static String getESAliasName(AtlasEntity entity) {
        String qualifiedName = getQualifiedName(entity);

        String[] parts = qualifiedName.split("/");

        return parts[parts.length - 1];
    }

    public static boolean getIsAllow(AtlasEntity entity) {
        return (boolean) entity.getAttribute("allow");
    }

    public static String getTenantId(AtlasEntity entity) {
        return (String) entity.getAttribute("tenantId");
    }

    public static boolean getIsEnabled(AtlasEntity entity) throws AtlasBaseException {
        return (boolean) entity.getAttribute("enabled");
    }

    public static List<String> getGroups(AtlasEntity entity) {
        return (List<String>) entity.getAttribute("groups");
    }

    public static List<String> getUsers(AtlasEntity entity) {
        return (List<String>) entity.getAttribute("users");
    }

    public static String getDisplayName(AtlasEntity entity) {
        return (String) entity.getAttribute("displayName");
    }

    public static String getDescription(AtlasEntity entity) {
        return (String) entity.getAttribute("description");
    }

    public static List<String> getActions(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("actions");
    }

    public static void validateUniquenessByName(EntityDiscoveryService entityDiscoveryService, String name, String typeName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("size", 1);

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        mustClauseList.add(mapOf("term", mapOf("name.keyword", name)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        AtlasSearchResult atlasSearchResult = entityDiscoveryService.directIndexSearch(indexSearchParams);

        if (CollectionUtils.isNotEmpty(atlasSearchResult.getEntities())){
            throw new AtlasBaseException(String.format("Entity already exists, typeName:name, %s:%s", typeName, name));
        }
    }

    public static void validateUniquenessByTags(EntityDiscoveryService entityDiscoveryService, List<String> tags, String typeName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("size", 1);

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        tags.forEach(x -> mustClauseList.add(mapOf("term", mapOf("tags", x))));


        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        AtlasSearchResult atlasSearchResult = entityDiscoveryService.directIndexSearch(indexSearchParams);

        if (CollectionUtils.isNotEmpty(atlasSearchResult.getEntities())){
            throw new AtlasBaseException(String.format("Entity already exists, typeName:tags, %s:%s", typeName, tags));
        }
    }

    public static RangerPolicy fetchRangerPolicyByResources(AtlasRangerService atlasRangerService,
                                                            String serviceType,
                                                            String policyType,
                                                            RangerPolicy policy) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        Map<String, String> resourceForSearch = new HashMap<>();
        for (String resourceName : policy.getResources().keySet()) {

            RangerPolicy.RangerPolicyResource value = policy.getResources().get(resourceName);
            resourceForSearch.put(resourceName, value.getValues().get(0));
        }

        LOG.info("resourceForSearch {}", AtlasType.toJson(resourceForSearch));

        Map <String, String> params = new HashMap<>();
        int size = 25;
        int from = 0;

        params.put("policyType", policyType); //POLICY_TYPE_ACCESS
        params.put("page", "0");
        params.put("pageSize", String.valueOf(size));
        params.put("serviceType", serviceType);

        int fetched;
        do {
            params.put("startIndex", String.valueOf(from));

            List<RangerPolicy> rangerPoliciesPaginated = atlasRangerService.getPoliciesByResources(resourceForSearch, params);
            fetched = rangerPoliciesPaginated.size();
            rangerPolicies.addAll(rangerPoliciesPaginated);

            from += size;

        } while (fetched == size);

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            //find exact match among the result list
            String provisionalPolicyResourcesSignature = new RangerPolicyResourceSignature(policy).getSignature();

            for (RangerPolicy resourceMatchedPolicy : rangerPolicies) {
                String resourceMatchedPolicyResourcesSignature = new RangerPolicyResourceSignature(resourceMatchedPolicy).getSignature();

                if (provisionalPolicyResourcesSignature.equals(resourceMatchedPolicyResourcesSignature) &&
                        Integer.valueOf(policyType).equals(resourceMatchedPolicy.getPolicyType()) &&
                        serviceType.equals(resourceMatchedPolicy.getServiceType())) {
                    return resourceMatchedPolicy;
                }
            }
        }

        return null;
    }


    public static List<RangerPolicy> fetchRangerPoliciesByLabel(AtlasRangerService atlasRangerService,
                                                                String serviceType,
                                                                String policyType,
                                                                String label) throws AtlasBaseException {
        List<RangerPolicy> ret = new ArrayList<>();

        Map <String, String> params = new HashMap<>();
        int size = 25;
        int from = 0;

        params.put("policyLabelsPartial", label);
        params.put("policyType", policyType); //POLICY_TYPE_ACCESS
        params.put("page", "0");
        params.put("pageSize", String.valueOf(size));
        params.put("serviceType", serviceType);

        int fetched;
        do {
            params.put("startIndex", String.valueOf(from));

            List<RangerPolicy> rangerPolicies = atlasRangerService.getPoliciesByLabel(params);
            fetched = rangerPolicies.size();
            ret.addAll(rangerPolicies);

            from += size;

        } while (fetched == size);

        return ret;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }
}
