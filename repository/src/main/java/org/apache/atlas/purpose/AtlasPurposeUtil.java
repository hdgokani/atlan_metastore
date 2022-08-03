package org.apache.atlas.purpose;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.PersonaPurposeCommonUtil;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;

public class AtlasPurposeUtil extends PersonaPurposeCommonUtil {

    public static final String RESOURCE_TAG = "tag";
    public static final String SERVICE_NAME = AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE.getString();



    public static List<String> getTags(AtlasEntity purpose) {
        return (List<String>) purpose.getAttribute("tags");
    }

    public static List<String> getTags(AtlasEntityHeader purpose) {
        return (List<String>) purpose.getAttribute("tags");
    }

    public static boolean getIsAllUsers(AtlasEntity policy) {
        return (boolean) policy.getAttribute("allUsers");
    }

    public static List<AtlasEntity> getPurposeAllPolicies(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        List<AtlasEntity> ret = new ArrayList<>();

        ret.addAll(getMetadataPolicies(entityWithExtInfo));
        //TODO: add data policies

        return ret;
    }

    public static List<AtlasEntity> getMetadataPolicies(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        List<AtlasEntity> ret = new ArrayList<>();
        AtlasEntity purpose = entityWithExtInfo.getEntity();

        List<AtlasObjectId> policies = (List<AtlasObjectId>) purpose.getRelationshipAttribute("metadataPolicies");

        if (policies != null) {
            ret = policies.stream()
                    .map(x -> entityWithExtInfo.getReferredEntity(x.getGuid()))
                    .filter(x -> x.getStatus() == AtlasEntity.Status.ACTIVE)
                    .collect(Collectors.toList());
        }

        return ret;
    }

    public static String getPurposeLabel(String purposeGuid) {
        return "purpose:" + purposeGuid;
    }

    public static String getPurposePolicyLabel(String purposePolicyGuid) {
        return "purpose:policy:" + purposePolicyGuid;
    }

    public static List<String> getLabelsForPurposePolicy(String purposeGuid, String purposePolicyGuid) {
        return Arrays.asList(getPurposeLabel(purposeGuid), getPurposePolicyLabel(purposePolicyGuid), "type:purpose");
    }

    protected static String formatAccessType(String type){
        return String.format("%s:%s", SERVICE_NAME, type);
    }

    protected static String getPurposeGuid(AtlasEntity policyEntity) {
        Object purpose = policyEntity.getRelationshipAttribute("purpose");
        if (purpose instanceof AtlasObjectId) {
            return ((AtlasObjectId) purpose).getGuid();
        } else if (purpose instanceof Map) {
            return (String) ((HashMap) purpose).get("guid");
        }

        return null;
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

            for (AtlasEntityHeader header : atlasSearchResult.getEntities()) {
                //TODO: handle via ES query if possible -> match exact tags list
                if (CollectionUtils.isEqualCollection(getTags(header), tags)) {
                    throw new AtlasBaseException(String.format("Entity already exists, typeName:tags, %s:%s", typeName, tags));
                }
            }
        }
    }
}

