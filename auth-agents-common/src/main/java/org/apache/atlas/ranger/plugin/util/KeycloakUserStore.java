/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.ranger.plugin.util;

import atlas.keycloak.client.KeycloakClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfMetrics;

import org.apache.commons.collections.CollectionUtils;
import org.apache.atlas.ranger.plugin.model.RangerRole;
import org.apache.atlas.ranger.plugin.service.RangerBasePlugin;
import org.keycloak.admin.client.resource.RoleResource;
import org.keycloak.representations.idm.AdminEventRepresentation;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class KeycloakUserStore {
    private static final Logger LOG = LoggerFactory.getLogger(KeycloakUserStore.class);

    private static int NUM_THREADS = 5;

    private final String serviceName;

    public KeycloakUserStore(String serviceName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerRolesProvider(serviceName=" + serviceName + ").RangerRolesProvider()");
        }

        this.serviceName = serviceName;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerRolesProvider(serviceName=" + serviceName + ").RangerRolesProvider()");
        }
    }

    public static ExecutorService getExecutorService(String namePattern) {
        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS,
                new ThreadFactoryBuilder().setNameFormat(namePattern + Thread.currentThread().getName())
                        .build());
        return service;
    }

    public long getKeycloakSubjectsStoreUpdatedTime() {

        //TODO: String vriables
        List<String> operationTypes = Arrays.asList("CREATE", "UPDATE", "DELETE");
        List<String> resourceTypes = Arrays.asList("USER", "GROUP", "REALM_ROLE", "REALM_ROLE_MAPPING", "GROUP_MEMBERSHIP");
        List<AdminEventRepresentation> adminEvents = KeycloakClient.getKeycloakClient().getRealm().getAdminEvents(operationTypes,
                null, null, null, null, null,
                resourceTypes,
                null, null,
                0,1);

        long latestEventTime = -1L;
        if (CollectionUtils.isNotEmpty(adminEvents)) {
            latestEventTime = adminEvents.get(0).getTime();
        }

        return latestEventTime;
    }

    public RangerRoles loadRolesIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadRoles");

        long keycloakUpdateTime = getKeycloakSubjectsStoreUpdatedTime();
        if (keycloakUpdateTime <= lastUpdatedTime) {
            return null;
        }

        List<RoleRepresentation> kRoles = KeycloakClient.getKeycloakClient().getAllRoles();
        LOG.info("Found {} keycloak roles", kRoles.size());

        Set<RangerRole> roleSet = new HashSet<>();
        RangerRoles rangerRoles = new RangerRoles();
        List<UserRepresentation> userNamesList = new ArrayList<>();

        submitCallablesAndWaitToFinish("RoleSubjectsFetcher",
                kRoles.stream()
                        .map(x -> new RoleSubjectsFetcher(x, roleSet, userNamesList))
                        .collect(Collectors.toList()));

        rangerRoles.setRangerRoles(roleSet);
        rangerRoles.setServiceName(serviceName);

        Date current = new Date();
        rangerRoles.setRoleUpdateTime(current);
        rangerRoles.setServiceName(serviceName);
        rangerRoles.setRoleVersion(-1L);

        RequestContext.get().endMetricRecord(recorder);

        return rangerRoles;
    }

    public RangerUserStore loadUserStoreIfUpdated(long lastUpdatedTime) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadUserStore");

        long keycloakUpdateTime = getKeycloakSubjectsStoreUpdatedTime();
        if (keycloakUpdateTime <= lastUpdatedTime) {
            return null;
        }

        Map<String, Set<String>> userGroupMapping = new HashMap<>();

        List<UserRepresentation> kUsers = KeycloakClient.getKeycloakClient().getAllUsers();
        LOG.info("Found {} keycloak users", kUsers.size());

        List<Callable<Object>> callables = new ArrayList<>();
        kUsers.forEach(x -> callables.add(new UserGroupsFetcher(x, userGroupMapping)));

        submitCallablesAndWaitToFinish("RoleSubjectsFetcher", callables);

        RangerUserStore userStore = new RangerUserStore();
        userStore.setUserGroupMapping(userGroupMapping);
        Date current = new Date();
        userStore.setUserStoreUpdateTime(current);
        userStore.setServiceName(serviceName);
        userStore.setUserStoreVersion(-1L);

        RequestContext.get().endMetricRecord(recorder);

        return userStore;
    }

    private static void loadUserStore(RangerBasePlugin plugin,
                                      List<UserRepresentation> userNamesList,
                                      RangerUserStoreProvider userStoreProvider) throws AtlasBaseException {

        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("loadUserStore");

        Map<String, Set<String>> userGroupMapping = new HashMap<>();

        Set<String> distUsers = new HashSet<>();
        List<Callable<Object>> callables = new ArrayList<>();
        for (UserRepresentation kUser : userNamesList) {
            if (!distUsers.contains(kUser.getUsername())) {
                distUsers.add(kUser.getUsername());
                callables.add(new UserGroupsFetcher(kUser, userGroupMapping));
            }
        }

        submitCallablesAndWaitToFinish("RoleSubjectsFetcher", callables);

        RangerUserStore userStore = new RangerUserStore();
        userStore.setUserGroupMapping(userGroupMapping);

        plugin.setUserStore(userStore);
        userStoreProvider.setRangerUserStoreSetInPlugin(true);

        Date current = new Date();
        userStore.setUserStoreUpdateTime(current);
        userStoreProvider.setLastActivationTimeInMillis(current.getTime());
        userStoreProvider.saveToCache(userStore);

        RequestContext.get().endMetricRecord(recorder);
    }


    private static RangerRole keycloakRoleToRangerRole(RoleRepresentation kRole) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakRolesToRangerRoles");

        RangerRole rangerRole = new RangerRole();
        //rangerRole.setId(kRole.getId());
        rangerRole.setName(kRole.getName());
        rangerRole.setDescription(kRole.getDescription() + " " + kRole.getId());
        //TODO: following properties
        //rangerRole.setOptions(kRole.getAttributes());
        //createdBy, updatedBy
        //createTime, updateTime
        //isEnabled
        //version

        RequestContext.get().endMetricRecord(recorder);
        return rangerRole;
    }

    private static List<RangerRole.RoleMember> keycloakGroupsToRangerRoleMember(Set<GroupRepresentation> kGroups) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakGroupsToRangerRoleMember");
        List<RangerRole.RoleMember> rangerGroups = new ArrayList<>();

        for (GroupRepresentation kGroup : kGroups) {
            //TODO: Revisit isAdmin flag
            rangerGroups.add(new RangerRole.RoleMember(kGroup.getName(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerGroups;
    }

    private static List<RangerRole.RoleMember> keycloakUsersToRangerRoleMember(Set<UserRepresentation> kUsers) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakUsersToRangerRoleMember");
        List<RangerRole.RoleMember> rangerUsers = new ArrayList<>();

        for (UserRepresentation kUser : kUsers) {
            //TODO: Revisit isAdmin flag
            rangerUsers.add(new RangerRole.RoleMember(kUser.getUsername(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerUsers;
    }

    private static List<RangerRole.RoleMember> keycloakRolesToRangerRoleMember(Set<RoleRepresentation> kRoles) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("keycloakRolesToRangerRoleMember");
        List<RangerRole.RoleMember> rangerRoles = new ArrayList<>();

        for (RoleRepresentation kRole : kRoles) {
            //TODO: Revisit isAdmin flag
            rangerRoles.add(new RangerRole.RoleMember(kRole.getName(), false));
        }

        RequestContext.get().endMetricRecord(recorder);
        return rangerRoles;
    }

    protected static <T> void submitCallablesAndWaitToFinish(String threadName, List<Callable<T>> callables) throws AtlasBaseException {
        ExecutorService service = getExecutorService(threadName + "-%d-");
        try {

            LOG.info("Submitting callables: {}", threadName);
            callables.forEach(service::submit);

            LOG.info("Shutting down executor: {}", threadName);
            service.shutdown();
            LOG.info("Shut down executor: {}", threadName);
            boolean terminated = service.awaitTermination(60, TimeUnit.SECONDS);
            LOG.info("awaitTermination done: {}", threadName);

            if (!terminated) {
                LOG.warn("Time out occurred while waiting to complete {}", threadName);
            }
        } catch (InterruptedException e) {
            throw new AtlasBaseException();
        }
    }

    static class RoleSubjectsFetcher implements Callable<RangerRole> {
        private Set<RangerRole> roleSet;
        private RoleRepresentation kRole;
        List<UserRepresentation> userNamesList;

        public RoleSubjectsFetcher(RoleRepresentation kRole,
                                   Set<RangerRole> roleSet,
                                   List<UserRepresentation> userNamesList) {
            this.kRole = kRole;
            this.roleSet = roleSet;
            this.userNamesList = userNamesList;
        }

        @Override
        public RangerRole call() throws Exception {
            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("roleSubjectsFetcher");

            RangerRole rangerRole = keycloakRoleToRangerRole(kRole);
            RoleResource roleResource = KeycloakClient.getKeycloakClient().getRealm().roles().get(kRole.getName());

            //get all groups for Roles
            Thread groupsFetcher = new Thread(() -> {
                int start = 0;
                int size = 100;
                Set<GroupRepresentation> ret = new HashSet<>();

                do {
                    Set<GroupRepresentation> kGroups = roleResource.getRoleGroupMembers(start, size);
                    ret.addAll(kGroups);
                    start += size;

                } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

                rangerRole.setGroups(keycloakGroupsToRangerRoleMember(ret));
            });
            groupsFetcher.start();

            //get all users for Roles
            Thread usersFetcher = new Thread(() -> {
                int start = 0;
                int size = 100;
                Set<UserRepresentation> ret = new HashSet<>();

                do {
                    Set<UserRepresentation> userRepresentations = roleResource.getRoleUserMembers(start, size);
                    ret.addAll(userRepresentations);
                    start += size;

                } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

                rangerRole.setUsers(keycloakUsersToRangerRoleMember(ret));
                userNamesList.addAll(ret);
            });
            usersFetcher.start();

            //get all roles for Roles
            Thread subRolesFetcher = new Thread(() -> {
                Set<RoleRepresentation> kSubRoles = roleResource.getRoleComposites();
                rangerRole.setRoles(keycloakRolesToRangerRoleMember(kSubRoles));
            });
            subRolesFetcher.start();

            try {
                groupsFetcher.join();
                usersFetcher.join();
                subRolesFetcher.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            RequestContext.get().endMetricRecord(recorder);
            roleSet.add(rangerRole);

            return rangerRole;
        }
    }

    static class UserGroupsFetcher implements Callable {
        private Map<String, Set<String>> userGroupMapping;
        private UserRepresentation kUser;

        public UserGroupsFetcher(UserRepresentation kUser, Map<String, Set<String>> userGroupMapping) {
            this.kUser = kUser;
            this.userGroupMapping = userGroupMapping;
        }

        @Override
        public Object call() throws Exception {
            AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("userGroupsFetcher");

            List<GroupRepresentation> kGroups = KeycloakClient.getKeycloakClient().getRealm().users().get(kUser.getId()).groups();
            userGroupMapping.put(kUser.getUsername(),
                    kGroups.stream()
                            .map(GroupRepresentation::getName)
                            .collect(Collectors.toSet()));

            RequestContext.get().endMetricRecord(recorder);

            return null;
        }
    }
}
