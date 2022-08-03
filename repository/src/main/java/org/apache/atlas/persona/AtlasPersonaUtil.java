package org.apache.atlas.persona;

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

import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class AtlasPersonaUtil extends PersonaPurposeCommonUtil {

    public static final String RESOURCE_KEY_ENTITY = "entity";
    public static final String RESOURCE_ENTITY_TYPE = "entity-type";
    public static final String RESOURCE_ENTITY_CLASS = "entity-classification";
    public static final String RESOURCE_ENTITY_LABEL = "entity-label";
    public static final String RESOURCE_BM = "entity-business-metadata";
    public static final String RESOURCE_CLASS = "classification";

    public static final String RESOURCE_REL_TYPE = "relationship-type";

    public static final String RESOURCE_END_ONE_ENTITY = "end-one-entity";
    public static final String RESOURCE_END_ONE_ENTITY_TYPE = "end-one-entity-type";
    public static final String RESOURCE_END_ONE_ENTITY_CLASS = "end-one-entity-classification";

    public static final String RESOURCE_END_TWO_ENTITY = "end-two-entity";
    public static final String RESOURCE_END_TWO_ENTITY_TYPE = "end-two-entity-type";
    public static final String RESOURCE_END_TWO_ENTITY_CLASS = "end-two-entity-classification";


    public static final List<String> ENTITY_ACTIONS = Arrays.asList(ACCESS_ENTITY_READ, "entity-create", "entity-update", "entity-delete");
    public static final List<String> CLASSIFICATION_ACTIONS = Arrays.asList("entity-add-classification", "entity-update-classification", "entity-remove-classification");
    public static final List<String> TERM_ACTIONS = Arrays.asList("add-terms", "remove-terms");
    public static final List<String> LABEL_ACTIONS = Arrays.asList("entity-add-label", "entity-update-label", "entity-remove-label");
    public static final String BM_ACTION = "entity-update-business-metadata";

    public static final List<String> GLOSSARY_TYPES = Arrays.asList(ATLAS_GLOSSARY_ENTITY_TYPE, ATLAS_GLOSSARY_TERM_ENTITY_TYPE, ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE);


    public static String getPersonaLabel(String personaGuid) {
        return "persona:" + personaGuid;
    }

    public static String getPersonaPolicyLabel(String personaPolicyGuid) {
        return "persona:policy:" + personaPolicyGuid;
    }

    public static List<String> getLabelsForPersonaPolicy(String personaGuid, String personaPolicyGuid) {
        return Arrays.asList(getPersonaLabel(personaGuid), getPersonaPolicyLabel(personaPolicyGuid), "type:persona");
    }

    public static String getRoleName(AtlasEntity personaEntity) {
        return getQualifiedName(personaEntity);
    }

    public static long getPersonaRoleId(AtlasEntity entity) throws AtlasBaseException {
        String roleId = (String) entity.getAttribute("rangerRoleId");
        if (roleId == null) {
            throw new AtlasBaseException("rangerRoleId not found for Persona with GUID " + entity.getGuid());
        }
        return Long.parseLong(roleId);
    }

    public static List<RangerRole.RoleMember> getGroupsAsRangerRole(AtlasEntity entity) {
        List<String> groups =  (List<String>) entity.getAttribute("groups");

        return groups.stream().map(x -> new RangerRole.RoleMember(x, false)).collect(Collectors.toList());
    }

    public static List<RangerRole.RoleMember> getUsersAsRangerRole(AtlasEntity entity) {
        List<String> users = (List<String>) entity.getAttribute("users");

        return users.stream().map(x -> new RangerRole.RoleMember(x, false)).collect(Collectors.toList());
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

    public static List<String> getAssets(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("assets");
    }

    public static List<String> getGlossaryQualifiedNames(AtlasEntity personaPolicyEntity) {
        return (List<String>) personaPolicyEntity.getAttribute("glossaryQualifiedNames");
    }

    public static List<AtlasEntity> getPersonaAllPolicies(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        List<AtlasEntity> ret = new ArrayList<>();

        ret.addAll(getMetadataPolicies(entityWithExtInfo));
        ret.addAll(getGlossaryPolicies(entityWithExtInfo));
        //TODO: add data policies

        return ret;
    }

    public static List<AtlasEntity> getMetadataPolicies(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        List<AtlasEntity> ret = new ArrayList<>();
        AtlasEntity persona = entityWithExtInfo.getEntity();

        List<AtlasObjectId> policies = (List<AtlasObjectId>) persona.getRelationshipAttribute("metadataPolicies");

        if (policies != null) {
            ret = policies.stream().map(x -> entityWithExtInfo.getReferredEntity(x.getGuid())).collect(Collectors.toList());
        }

        return ret;
    }

    public static List<AtlasEntity> getGlossaryPolicies(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        List<AtlasEntity> ret = new ArrayList<>();
        AtlasEntity persona = entityWithExtInfo.getEntity();

        List<AtlasObjectId> policies = (List<AtlasObjectId>) persona.getRelationshipAttribute("glossaryPolicies");

        if (policies != null) {
            ret = policies.stream().map(x -> entityWithExtInfo.getReferredEntity(x.getGuid())).collect(Collectors.toList());
        }

        return ret;
    }
}
