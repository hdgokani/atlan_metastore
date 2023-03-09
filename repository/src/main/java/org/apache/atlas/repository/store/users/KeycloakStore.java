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

package org.apache.atlas.repository.store.users;

import org.apache.atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.RoleByIdResource;
import org.keycloak.admin.client.resource.RoleResource;
import org.keycloak.admin.client.resource.RolesResource;
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
import java.util.stream.Collectors;

import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaGroups;

public class KeycloakStore {
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakStore.class);

    private boolean saveUsersToAttributes = false;
    private boolean saveGroupsToAttributes = false;

    public KeycloakStore() {

    }

    public KeycloakStore(boolean saveUsersToAttributes, boolean saveGroupsToAttributes) {
        this.saveUsersToAttributes  = saveUsersToAttributes;
        this.saveGroupsToAttributes  = saveGroupsToAttributes;
    }


    public RoleRepresentation createRole(String name) throws AtlasBaseException {
        return createRole(name, false, null, null, null, null);
    }

    public RoleRepresentation createRole(String name,
                                         List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {
        return createRole(name, false, users, groups, roles, null);
    }

    public RoleRepresentation createRoleForConnection(String name, boolean isComposite,
                                         List<String> users, List<String> groups, List<String> roles) throws AtlasBaseException {

        RolesResource rolesResource      = KeycloakClient.getKeycloakClient().getRealm().roles();
        RoleByIdResource rolesIdResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();

        List<UserRepresentation> roleUsers = new ArrayList<>();
        UsersResource usersResource = null;

        if (CollectionUtils.isNotEmpty(users)) {
            usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

            for (String userName : users) {
                List<UserRepresentation> matchedUsers = usersResource.search(userName);
                Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

                if (keyUserOptional.isPresent()) {
                    roleUsers.add(keyUserOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak user not found with userName " + userName);
                }
            }
        }

        List<GroupRepresentation> roleGroups = new ArrayList<>();
        GroupsResource groupsResource = null;

        if (CollectionUtils.isNotEmpty(groups)) {
            groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

            for (String groupName : groups) {
                List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

                if (keyGroupOptional.isPresent()) {
                    roleGroups.add(keyGroupOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak group not found with name " + groupName);
                }
            }
        }

        //TODO: remove discrepency to pass roleName instead of roleId
        List<RoleRepresentation> roleRoles = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(roles)) {
            for (String roleId : roles) {
                roleRoles.add(rolesIdResource.getRole(roleId));
            }
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(name);
        role.setComposite(isComposite);

        RoleRepresentation createdRole = createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException("Failed to create a keycloak role " + name);
        }
        LOG.info("Created keycloak role with name {}", name);

        //add realm role into users
        if (CollectionUtils.isNotEmpty(roleUsers)) {
            for (UserRepresentation kUser : roleUsers) {
                UserResource userResource = usersResource.get(kUser.getId());

                userResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                userResource.update(kUser);
            }
        }

        //add realm role into groups
        if (CollectionUtils.isNotEmpty(roleGroups)) {
            for (GroupRepresentation kGroup : roleGroups) {
                GroupResource groupResource = groupsResource.group(kGroup.getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                groupResource.update(kGroup);
            }
        }

        //add realm role into roles
        if (CollectionUtils.isNotEmpty(roleRoles)) {
            RoleResource connectionRoleResource = rolesResource.get(createdRole.getName());

            for (RoleRepresentation kRole : roleRoles) {
                RoleResource roleResource = rolesResource.get(kRole.getName());

                connectionRoleResource.addComposites(Collections.singletonList(roleResource.toRepresentation()));
                connectionRoleResource.update(connectionRoleResource.toRepresentation());
            }
        }

        return createdRole;

        //return createRole(name, isComposite, users, groups, roles, null);
    }

    public RoleRepresentation createRole(String name, boolean isComposite,
                                         List<String> users, List<String> groups, List<String> roles,
                                         Map<String, List<String>> attributes) throws AtlasBaseException {

        RolesResource rolesResource = KeycloakClient.getKeycloakClient().getRealm().roles();

        List<UserRepresentation> roleUsers = new ArrayList<>();
        UsersResource usersResource = null;

        if (CollectionUtils.isNotEmpty(users)) {
            usersResource = KeycloakClient.getKeycloakClient().getRealm().users();

            for (String userName : users) {
                List<UserRepresentation> matchedUsers = usersResource.search(userName);
                Optional<UserRepresentation> keyUserOptional = matchedUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();

                if (keyUserOptional.isPresent()) {
                    roleUsers.add(keyUserOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak user not found with userName " + userName);
                }
            }
        }

        List<GroupRepresentation> roleGroups = new ArrayList<>();
        GroupsResource groupsResource = null;

        if (CollectionUtils.isNotEmpty(groups)) {
            groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

            for (String groupName : groups) {
                List<GroupRepresentation> matchedGroups = groupsResource.groups(groupName, 0, 100);
                Optional<GroupRepresentation> keyGroupOptional = matchedGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();

                if (keyGroupOptional.isPresent()) {
                    roleGroups.add(keyGroupOptional.get());
                } else {
                    throw new AtlasBaseException("Keycloak group not found with name " + groupName);
                }
            }
        }

        List<RoleRepresentation> roleRoles = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(roles)) {
            for (String roleName : roles) {
                LOG.info("Searching role {}", roleName);
                RoleRepresentation roleRepresentation = rolesResource.get(roleName).toRepresentation();

                if (roleRepresentation != null) {
                    roleRoles.add(roleRepresentation);
                } else {
                    throw new AtlasBaseException("Keycloak role not found with name " + roleName);
                }
            }
        }

        RoleRepresentation role = new RoleRepresentation();
        role.setName(name);
        role.setComposite(isComposite);

        if (attributes == null) {
            attributes = new HashMap<>();
        }

        if (saveUsersToAttributes) {
            attributes.put("users", Collections.singletonList(AtlasType.toJson(roleUsers.stream().map(x -> x.getId()).collect(Collectors.toList()))));
        }

        if (saveGroupsToAttributes) {
            attributes.put("groups", Collections.singletonList(AtlasType.toJson(roleGroups.stream().map(x -> x.getId()).collect(Collectors.toList()))));
        }

        if (MapUtils.isNotEmpty(attributes)) {
            role.setAttributes(attributes);
        }

        RoleRepresentation createdRole = createRole(role);
        if (createdRole == null) {
            throw new AtlasBaseException("Failed to create a keycloak role " + name);
        }
        LOG.info("Created keycloak role with name {}", name);

        //add realm role into users
        if (CollectionUtils.isNotEmpty(roleUsers)) {
            for (UserRepresentation kUser : roleUsers) {
                UserResource userResource = usersResource.get(kUser.getId());

                userResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                userResource.update(kUser);
            }
        }

        //add realm role into groups
        if (CollectionUtils.isNotEmpty(roleGroups)) {
            for (GroupRepresentation kGroup : roleGroups) {
                GroupResource groupResource = groupsResource.group(kGroup.getId());

                groupResource.roles().realmLevel().add(Collections.singletonList(createdRole));
                groupResource.update(kGroup);
            }
        }

        //add realm role into roles
        if (CollectionUtils.isNotEmpty(roleRoles)) {
            RoleResource connectionRoleResource = rolesResource.get(createdRole.getName());

            for (RoleRepresentation kRole : roleRoles) {
                RoleResource roleResource = rolesResource.get(kRole.getName());

                connectionRoleResource.addComposites(Collections.singletonList(roleResource.toRepresentation()));
                connectionRoleResource.update(connectionRoleResource.toRepresentation());
            }
        }

        return createdRole;
    }

    public RoleRepresentation createRole(RoleRepresentation role) {
        KeycloakClient.getKeycloakClient().getRealm().roles().create(role);

        return KeycloakClient.getKeycloakClient().getRealm()
                .roles()
                .get(role.getName())
                .toRepresentation();
    }

    public void updateRoleUsers(String roleId, String roleName,
                                List<String> existingUsers, List<String> newUsers,
                                RoleByIdResource rolesResource, RoleRepresentation roleRepresentation) throws AtlasBaseException {

        List<String> usersToAdd     = (List<String>) CollectionUtils.removeAll(newUsers, existingUsers);
        List<String> usersToRemove  = (List<String>) CollectionUtils.removeAll(existingUsers, newUsers);

        if (rolesResource == null) {
            rolesResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();
        }

        if (roleRepresentation == null) {
            roleRepresentation = rolesResource.getRole(roleId);
        }

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
    }

    public void updateRoleGroups(String roleId, String roleName,
                                List<String> existingGroups, List<String> newGroups,
                                RoleByIdResource rolesResource, RoleRepresentation roleRepresentation) throws AtlasBaseException {

        if (rolesResource == null) {
            rolesResource = KeycloakClient.getKeycloakClient().getRealm().rolesById();
        }

        if (roleRepresentation == null) {
            roleRepresentation = rolesResource.getRole(roleId);
        }

        GroupsResource groupsResource = KeycloakClient.getKeycloakClient().getRealm().groups();

        List<String> groupsToAdd    = (List<String>) CollectionUtils.removeAll(newGroups, existingGroups);
        List<String> groupsToRemove = (List<String>) CollectionUtils.removeAll(existingGroups, newGroups);

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
    }
}
