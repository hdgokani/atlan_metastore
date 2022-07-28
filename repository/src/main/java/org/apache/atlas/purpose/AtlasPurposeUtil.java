package org.apache.atlas.purpose;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.PersonaPurposeCommonUtil;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.util.NanoIdUtils;
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
            ret = policies.stream().map(x -> entityWithExtInfo.getReferredEntity(x.getGuid())).collect(Collectors.toList());
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
}

