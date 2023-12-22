package org.apache.atlas.authorizer;

import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorizerCommon {

    public static final String POLICY_TYPE_ALLOW = "allow";
    public static final String POLICY_TYPE_DENY = "deny";

    private static AtlasTypeRegistry typeRegistry;

    public static void setTypeRegistry(AtlasTypeRegistry typeRegistry) {
        AuthorizerCommon.typeRegistry = typeRegistry;
    }

    public static String getCurrentUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null ? auth.getName() : "";
    }

    public static boolean arrayListContains(List<String> listA, List<String> listB) {
        for (String listAItem : listA){
            if (listB.contains(listAItem)) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static boolean listStartsWith(String value, List<String> list) {
        for (String item : list){
            if (item.startsWith(value)) {
                return true;
            }
        }
        return false;
    }

    public static boolean listEndsWith(String value, List<String> list) {
        for (String item : list){
            if (item.endsWith(value)) {
                return true;
            }
        }
        return false;
    }

    public static Set<String> getTypeAndSupertypesList(String typeName) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

        if (entityType == null) {
            return Collections.singleton(typeName);
        } else {
            return typeRegistry.getEntityTypeByName(typeName).getTypeAndAllSuperTypes();
        }
    }

    public static AtlasEntityType getEntityTypeByName(String typeName) {
        return typeRegistry.getEntityTypeByName(typeName);
    }

}
