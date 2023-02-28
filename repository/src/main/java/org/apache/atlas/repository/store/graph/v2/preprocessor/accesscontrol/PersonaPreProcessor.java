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
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;


import atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.RequestContext;
import org.apache.atlas.aliasstore.ESAliasStore;
import org.apache.atlas.aliasstore.IndexAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.RoleByIdResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_ROLE_ID;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaGroups;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleId;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaUsers;
import static org.apache.atlas.repository.util.AccessControlUtils.getTenantId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;

public class PersonaPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaPreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;

    public PersonaPreProcessor(AtlasGraph graph,
                               AtlasTypeRegistry typeRegistry,
                               EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;

        aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PersonaPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePersona(entity);
                break;
            case UPDATE:
                processUpdatePersona(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePersona(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePersona");

        String tenantId = getTenantId(entity);

        entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

        //create keycloak role
        String roleId = createKeycloakRole((AtlasEntity) entity);

        entity.setAttribute(ATTR_PERSONA_ROLE_ID, roleId);

        //create ES alias
        aliasStore.createAlias((AtlasEntity) entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePersona(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePersona");

        AtlasEntity persona = (AtlasEntity) entity;

        AtlasEntity.AtlasEntityWithExtInfo existingPersonaEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex);

        if (!AtlasEntity.Status.ACTIVE.equals(existingPersonaEntity.getEntity().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona not Active");
        }

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);
        entity.setAttribute(ATTR_PERSONA_ROLE_ID, getPersonaRoleId(existingPersonaEntity.getEntity()));


        boolean isEnabled = getIsEnabled(persona);
        if (getIsEnabled(existingPersonaEntity.getEntity()) != isEnabled) {
            if (isEnabled) {
                //TODO:enablePersona(existingPersonaWithExtInfo);
            } else {
                //TODO:disablePersona(existingPersonaWithExtInfo);
            }
        }

        /*if (!getName(persona).equals(getName(existingPersonaEntity))) {
            validateUniquenessByName(graph, getName(persona), PERSONA_ENTITY_TYPE);
        }*/

        updateKeycloakRole(persona, existingPersonaEntity.getEntity());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName() {
        return getUUID();
    }

    private String createKeycloakRole(AtlasEntity entity) throws AtlasBaseException {
        String roleName = (String) entity.getAttribute(NAME);
        List<String> users = getPersonaUsers(entity);
        List<String> groups = getPersonaGroups(entity);

        List<UserRepresentation> keycloakPersonaUsers = new ArrayList<>();
        UsersResource usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

        for (String userName : users) {
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

            if (keyUserOptional.isPresent()) {
                keycloakPersonaUsers.add(keyUserOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak user not found with userName " + userName);
            }
        }

        List<GroupRepresentation> keycloakPersonaGroups = new ArrayList<>();
        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

        for (String groupName : groups) {
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

            if (keyGroupOptional.isPresent()) {
                keycloakPersonaGroups.add(keyGroupOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak group not found with name " + groupName);
            }
        }

        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("name", Collections.singletonList(roleName));
        attributes.put("type", Collections.singletonList("persona"));
        attributes.put("level", Collections.singletonList("workspace"));
        attributes.put("createdAt", Collections.singletonList(String.valueOf(System.currentTimeMillis())));
        attributes.put("createdBy", Collections.singletonList(RequestContext.get().getUser()));
        attributes.put("enabled", Collections.singletonList(String.valueOf(true)));
        attributes.put("users", users);
        attributes.put("groups", groups);

        RoleRepresentation roleRepresentation = new RoleRepresentation();
        roleRepresentation.setName(roleName);
        roleRepresentation.setComposite(false);
        roleRepresentation.setAttributes(attributes);

        KeycloakClient.getKeycloakClient().getRealm().roles().create(roleRepresentation);
        LOG.info("Created keycloak role with name {}", roleName);

        String roleId = KeycloakClient.getKeycloakClient().getRealm().roles().get(roleName).toRepresentation().getId();
        roleRepresentation.setId(roleId);


        //add realm role into users
        for (UserRepresentation kUser : keycloakPersonaUsers) {
            final UserResource userResource = usersResource.get(kUser.getId());

            userResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
            userResource.update(kUser);
        }

        //add realm role into groups
        for (GroupRepresentation kGroup : keycloakPersonaGroups) {
            final GroupResource groupResource = groupsResource.group(kGroup.getId());

            groupResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
            groupResource.update(kGroup);
        }

        return roleId;
    }

    private void updateKeycloakRole(AtlasEntity newPersona, AtlasEntity existingPersona) throws AtlasBaseException {
        String roleId = getPersonaRoleId(existingPersona);
        String roleName = (String) existingPersona.getAttribute(NAME);

        List<String> newUsers       = getPersonaUsers(newPersona);
        List<String> newGroups      = getPersonaGroups(newPersona);
        List<String> existingUsers  = getPersonaUsers(existingPersona);
        List<String> existingGroups = getPersonaGroups(existingPersona);

        List<String> usersToAdd     = (List<String>) CollectionUtils.removeAll(newUsers, existingUsers);
        List<String> usersToRemove  = (List<String>) CollectionUtils.removeAll(existingUsers, newUsers);
        List<String> groupsToAdd    = (List<String>) CollectionUtils.removeAll(newGroups, existingGroups);
        List<String> groupsToRemove = (List<String>) CollectionUtils.removeAll(existingGroups, newGroups);


        RoleByIdResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();
        RoleRepresentation roleRepresentation = rolesResource.getRole(roleId);

        UsersResource usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

        for (String userName : usersToAdd) {
            LOG.info("Adding user {} to role {}", userName, roleName);
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

            if (keyUserOptional.isPresent()) {
                final UserResource userResource = usersResource.get(keyUserOptional.get().getId());

                userResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
                userResource.update(keyUserOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak user not found with userName " + userName);
            }
        }

        for (String userName : usersToRemove) {
            LOG.info("Removing user {} from role {}", userName, roleName);
            List<UserRepresentation> matchedUsers = usersResource.search(userName);
            Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

            if (keyUserOptional.isPresent()) {
                final UserResource userResource = usersResource.get(keyUserOptional.get().getId());

                userResource.roles().realmLevel().remove(Collections.singletonList(roleRepresentation));
                userResource.update(keyUserOptional.get());
            } else {
                LOG.warn("Keycloak user not found with userName " + userName);
            }
        }

        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

        for (String groupName : groupsToAdd) {
            LOG.info("Adding group {} to role {}", groupName, roleName);
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

            if (keyGroupOptional.isPresent()) {
                final GroupResource groupResource = groupsResource.group(keyGroupOptional.get().getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));
                groupResource.update(keyGroupOptional.get());
            } else {
                throw new AtlasBaseException("Keycloak group not found with userName " + groupName);
            }
        }

        for (String groupName : groupsToRemove) {
            LOG.info("removing group {} from role {}", groupName, roleName);
            List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
            Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

            if (keyGroupOptional.isPresent()) {
                final GroupResource groupResource = groupsResource.group(keyGroupOptional.get().getId());

                groupResource.roles().realmLevel().remove(Collections.singletonList(roleRepresentation));
                groupResource.update(keyGroupOptional.get());
            } else {
                LOG.warn("Keycloak group not found with userName " + groupName);
            }
        }

        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("updatedAt", Collections.singletonList(String.valueOf(System.currentTimeMillis())));
        attributes.put("updatedBy", Collections.singletonList(RequestContext.get().getUser()));
        attributes.put("enabled", Collections.singletonList(String.valueOf(true)));
        attributes.put("users", newUsers);
        attributes.put("groups", newGroups);

        roleRepresentation.setAttributes(attributes);

        rolesResource.updateRole(roleId, roleRepresentation);
        LOG.info("Updated keycloak role with name {}", roleName);
    }

    private AtlasEntity.AtlasEntityWithExtInfo getAccessControlEntity(AtlasEntity entity, EntityMutations.EntityOperation op) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("AccessControlPolicyPreProcessor.getAccessControl");
        AtlasEntity.AtlasEntityWithExtInfo ret;

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        ret = entityRetriever.toAtlasEntityWithExtInfo(objectId);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }
}
