package org.apache.atlas.purpose;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.*;

public class AtlasPurposeUtil {

    public static String RESOURCE_PREFIX = "resource:";

    public static String RESOURCE_KEY_ENTITY = "entity";
    public static String RESOURCE_ENTITY_TYPE = "entity-type";
    public static String RESOURCE_ENTITY_CLASS = "entity-classification";
    public static String RESOURCE_ENTITY_LABEL = "entity-label";
    public static String RESOURCE_BM = "entity-business-metadata";
    public static String RESOURCE_CLASS = "classification";

    public static String RESOURCE_REL_TYPE = "relationship-type";

    public static String RESOURCE_END_ONE_ENTITY = "end-one-entity";
    public static String RESOURCE_END_ONE_ENTITY_TYPE = "end-one-entity-type";
    public static String RESOURCE_END_ONE_ENTITY_CLASS = "end-one-entity-classification";

    public static String RESOURCE_END_TWO_ENTITY = "end-two-entity";
    public static String RESOURCE_END_TWO_ENTITY_TYPE = "end-two-entity-type";
    public static String RESOURCE_END_TWO_ENTITY_CLASS = "end-two-entity-classification";

    public static String ACCESS_ADD_REL = "add-relationship";
    public static String ACCESS_UPDATE_REL = "update-relationship";
    public static String ACCESS_REMOVE_REL = "remove-relationship";

    public static final List<String> ENTITY_ACTIONS = Arrays.asList("entity-read", "entity-create", "entity-update", "entity-delete");
    public static final List<String> CLASSIFICATION_ACTIONS = Arrays.asList("entity-add-classification", "entity-update-classification", "entity-remove-classification");
    public static final List<String> TERM_ACTIONS = Arrays.asList("add-terms", "remove-terms");
    public static final List<String> LABEL_ACTIONS = Arrays.asList("entity-add-label", "entity-update-label", "entity-remove-label");
    public static final String BM_ACTION = "entity-update-business-metadata";
    public static final String LINK_ASSET_ACTION = "link-assets";

    public static final List<String> GLOSSARY_TYPES = Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE, ATLAS_GLOSSARY_TERM_ENTITY_TYPE, ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE);


    public static String getUUID() {
        return NanoIdUtils.randomNanoId(22);
    }

    public static String getPersonaLabel(String personaGuid) {
        return "persona:" + personaGuid;
    }

    public static String getPersonaPolicyLabel(String personaPolicyGuid) {
        return "persona:policy:" + personaPolicyGuid;
    }

    public static String getName(AtlasEntity entity) {
        return (String) entity.getAttribute(NAME);
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return (String) entity.getAttribute(QUALIFIED_NAME);
    }

    public static boolean getIsAllow(AtlasEntity entity) {
        return (boolean) entity.getAttribute("allow");
    }

    public static String getRoleName(AtlasEntity personaEntity) {
        return (String) personaEntity.getAttribute(QUALIFIED_NAME);
    }

    public static long getPersonaRoleId(AtlasEntity entity) throws AtlasBaseException {
        String roleId = (String) entity.getAttribute("rangerRoleId");
        if (roleId == null) {
            throw new AtlasBaseException("rangerRoleId not found for Persona with GUID " + entity.getGuid());
        }
        return Long.parseLong(roleId);
    }

    public static boolean getIsEnabled(AtlasEntity entity) throws AtlasBaseException {
        return (boolean) entity.getAttribute("enabled");
    }

    public static List<String> getGroups(AtlasEntity entity) {
        return (List<String>) entity.getAttribute("groups");
    }

    public static List<RangerRole.RoleMember> getGroupsAsRangerRole(AtlasEntity entity) {
        List<String> groups =  (List<String>) entity.getAttribute("groups");

        return groups.stream().map(x -> new RangerRole.RoleMember(x, false)).collect(Collectors.toList());
    }

    public static List<String> getUsers(AtlasEntity entity) {
        return (List<String>) entity.getAttribute("users");
    }

    public static List<RangerRole.RoleMember> getUsersAsRangerRole(AtlasEntity entity) {
        List<String> users = (List<String>) entity.getAttribute("users");

        return users.stream().map(x -> new RangerRole.RoleMember(x, false)).collect(Collectors.toList());
    }

    public static String getDisplayName(AtlasEntity entity) {
        return (String) entity.getAttribute("displayName");
    }

    public static String getDescription(AtlasEntity entity) {
        return (String) entity.getAttribute("description");
    }

    public static String getConnectionId(AtlasEntity personaPolicyEntity) {
        return (String) personaPolicyEntity.getAttribute("connectionId");
    }

    public static String getPersonaGuid(AtlasEntity personaPolicyEntity) {
        Object persona = personaPolicyEntity.getRelationshipAttribute("persona");
        if (persona instanceof AtlasObjectId) {
            return ((AtlasObjectId) persona).getGuid();
        } else if (persona instanceof Map) {
            return (String) ((HashMap) persona).get("guid");
        }

        return null;
    }

    public static List<String> getActions(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("actions");
    }

    public static List<String> getAssets(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("assets");
    }

    public static List<String> getGlossaryQualifiedNames(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("glossaryQualifiedNames");
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }
}
