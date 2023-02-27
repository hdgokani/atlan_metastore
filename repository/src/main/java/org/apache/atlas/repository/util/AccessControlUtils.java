/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.util;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.util.NanoIdUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.ATTR_TENANT_ID;
import static org.apache.atlas.repository.Constants.CONNECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.DEFAULT_TENANT_ID;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;

public class AccessControlUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AccessControlUtils.class);

    public static final String ATTR_ACCESS_CONTROL_ENABLED = "isAccessControlEnabled";
    public static final String ATTR_ACCESS_CONTROL_DENY_CM_GUIDS = "denyCustomMetadataGuids";
    public static final String ATTR_ACCESS_CONTROL_DENY_ASSET_TABS = "denyAssetTabs";

    public static final String ATTR_PERSONA_ROLE_ID = "roleId";
    public static final String ATTR_PERSONA_USERS   = "personaUsers";
    public static final String ATTR_PERSONA_GROUPS  = "personaGroups";

    public static final String ATTR_PURPOSE_CLASSIFICATIONS  = "purposeClassifications";

    public static final String ATTR_POLICY_TYPE  = "policyType";
    public static final String ATTR_POLICY_ACTIONS  = "policyActions";
    public static final String ATTR_POLICY_RESOURCES  = "policyResources";

    public static final String REL_ATTR_ACCESS_CONTROL = "accessControl";
    public static final String REL_ATTR_POLICIES       = "policies";

    public static final String POLICY_TYPE_ALLOW  = "allow";
    public static final String POLICY_TYPE_DENY  = "deny";

    public static final String ACCESS_READ_PURPOSE_METADATA = "entity-read";
    public static final String ACCESS_READ_PERSONA_METADATA = "persona-asset-read";
    public static final String ACCESS_READ_PURPOSE_GLOSSARY = "persona-glossary-read";

    public static String getTenantId(AtlasStruct entity) {
        String ret = DEFAULT_TENANT_ID;

        Object tenantId = entity.getAttribute(ATTR_TENANT_ID);

        if (tenantId != null) {
            String tenantIdAsString = (String) tenantId;
            if (tenantIdAsString.length() > 0) {
                ret = tenantIdAsString;
            }
        }

        return ret;
    }

    public static long getPersonaRoleId(AtlasEntity entity) throws AtlasBaseException {
        String roleId = (String) entity.getAttribute(ATTR_PERSONA_ROLE_ID);
        if (roleId == null) {
            throw new AtlasBaseException("rangerRoleId not found for Persona with GUID " + entity.getGuid());
        }
        return Long.parseLong(roleId);
    }

    public static boolean getIsEnabled(AtlasEntity entity) throws AtlasBaseException {
        return (boolean) entity.getAttribute(ATTR_ACCESS_CONTROL_ENABLED);
    }

    public static List<String> getPersonaUsers(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PERSONA_USERS);
    }

    public static List<String> getPersonaGroups(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PERSONA_GROUPS);
    }

    public static List<String> getPurposeTags(AtlasStruct entity) {
        return getListAttribute(entity, ATTR_PURPOSE_CLASSIFICATIONS);
    }

    public static String getUUID(){
        return NanoIdUtils.randomNanoId(22);
    }

    public static String getESAliasName(AtlasEntity entity) {
        String qualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);

        String[] parts = qualifiedName.split("/");

        return parts[parts.length - 1];
    }

    public static boolean getIsAllow(AtlasEntity policyEntity) throws AtlasBaseException {
        String policyType = (String) policyEntity.getAttribute(ATTR_POLICY_TYPE);

        if (POLICY_TYPE_ALLOW.equals(policyType)) {
            return true;
        } else if (POLICY_TYPE_DENY.equals(policyType)) {
            return false;
        } else {
            throw new AtlasBaseException("Unsuppported policy type while creating index alias filters");
        }
    }

    public static List<String> getPolicyAssets(AtlasEntity policyEntity) throws AtlasBaseException {
        List<String> resources = getListAttribute(policyEntity, ATTR_POLICY_RESOURCES);

        List<String> assets = resources.stream()
                .filter(x -> x.startsWith("entity:"))
                .map(x -> x.split(":")[0])
                .collect(Collectors.toList());

        return assets;
    }

    public static List<String> getPolicyActions(AtlasEntity policyEntity) throws AtlasBaseException {
        List<String> actions = getListAttribute(policyEntity, ATTR_POLICY_ACTIONS);

        return actions;
    }

    public static Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }

    public static List<AtlasEntity> getPolicies(AtlasEntity.AtlasEntityWithExtInfo accessControl) {
        List<AtlasObjectId> policies = (List<AtlasObjectId>) accessControl.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);

        return objectToEntityList(accessControl, policies);
    }

    public static List<AtlasEntity> objectToEntityList(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo, List<AtlasObjectId> policies) {
        List<AtlasEntity> ret = new ArrayList<>();

        if (policies != null) {
            ret = policies.stream()
                    .map(x -> entityWithExtInfo.getReferredEntity(x.getGuid()))
                    .filter(x -> x.getStatus() == null || x.getStatus() == AtlasEntity.Status.ACTIVE)
                    .collect(Collectors.toList());
        }

        return ret;
    }

    public static String getConnectionQualifiedNameFromPolicyAssets(EntityGraphRetriever entityRetriever, List<String> assets) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Policy assets could not be null");
        }

        String[] splitted = assets.get(0).split("/");
        String connectionQName;
        try {
            connectionQName = splitted[0] + splitted[0] + splitted[2];
        } catch (ArrayIndexOutOfBoundsException aib) {
            throw new AtlasBaseException("Failed to extract qualifiedName of the connection");
        }

        if (getConnectionEntity(entityRetriever, connectionQName) != null) {
            return connectionQName;
        }

        throw new AtlasBaseException("Could not find connection for policy");
    }

    private static AtlasEntity getConnectionEntity(EntityGraphRetriever entityRetriever, String connectionQualifiedName) throws AtlasBaseException {
        AtlasObjectId objectId = new AtlasObjectId(CONNECTION_ENTITY_TYPE, mapOf(QUALIFIED_NAME, connectionQualifiedName));

        AtlasEntity entity = entityRetriever.toAtlasEntity(objectId);

        return entity;
    }

    private static List<String> getListAttribute(AtlasStruct entity, String attrName) {
        List<String> ret = new ArrayList<>();

        Object valueObj = entity.getAttribute(attrName);
        if (valueObj != null) {
            ret = (List<String>) valueObj;
        }

        return ret;
    }
}
