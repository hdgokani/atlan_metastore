/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.authcache;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.model.authcache.AuthzCacheRefreshInfo;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AuthzCacheRefresher;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.ATTR_ADMIN_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_ROLES;
import static org.apache.atlas.repository.Constants.ATTR_ADMIN_USERS;
import static org.apache.atlas.repository.Constants.ATTR_VIEWER_GROUPS;
import static org.apache.atlas.repository.Constants.ATTR_VIEWER_USERS;
import static org.apache.atlas.repository.Constants.COLLECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.CONNECTION_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_GROUPS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SUB_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_USERS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PURPOSE_CLASSIFICATIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_DATA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_SUB_CATEGORY_METADATA;


public class AuthzCacheOnDemandRefresher extends GraphTransactionInterceptor.PostTransactionHook {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzCacheOnDemandRefresher.class);

    private final AuthzCacheRefresher hostRefresher;

    private boolean isOnDemandAuthzCacheRefreshEnabled = true;

    private AsyncCacheRefresher asyncCacheRefresher;

    public AuthzCacheOnDemandRefresher() {
        super();
        this.hostRefresher = new AuthzCacheRefresher();

        try {
            isOnDemandAuthzCacheRefreshEnabled = ApplicationProperties.get().getBoolean("atlas.authorizer.enable.action.based.cache.refresh", true);
        } catch (AtlasException e) {
            LOG.warn("Property atlas.authorizer.enable.action.based.cache.refresh not found, default is true");
        }
    }

    private static final Set<String> CONNECTION_ATTRS = new HashSet<String>(){{
        add(ATTR_ADMIN_USERS);
        add(ATTR_ADMIN_GROUPS);
        add(ATTR_ADMIN_ROLES);
    }};

    private static final Set<String> COLLECTION_ATTRS = new HashSet<String>(){{
        add(ATTR_ADMIN_USERS);
        add(ATTR_ADMIN_GROUPS);
        add(ATTR_ADMIN_ROLES);
        add(ATTR_VIEWER_USERS);
        add(ATTR_VIEWER_GROUPS);
    }};

    private static final Set<String> PERSONA_ATTRS_ROLES = new HashSet<String>(){{
        add(ATTR_POLICY_USERS);
        add(ATTR_POLICY_GROUPS);
    }};

    private static final Set<String> PERSONA_ATTRS_POLICIES = new HashSet<String>(){{
        add(ATTR_ACCESS_CONTROL_ENABLED);
    }};

    private static final Set<String> PURPOSE_ATTRS = new HashSet<String>(){{
        add(ATTR_ACCESS_CONTROL_ENABLED);
        add(ATTR_PURPOSE_CLASSIFICATIONS);
    }};

    public void refreshCacheIfNeeded(EntityMutationResponse entityMutationResponse, boolean isImport) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("refreshCacheIfNeeded");

        try {
            if (isImport || RequestContext.get().isPoliciesBootstrappingInProgress()) {
                LOG.warn("refreshCacheIfNeeded: Cache refresh will be skipped");
                return;
            }

            if (!isOnDemandAuthzCacheRefreshEnabled) {
                LOG.warn("refreshCacheIfNeeded: Skipping as On-demand cache refresh is not enabled");
                return;
            }

            AsyncCacheRefresher asyncCacheRefresher = new AsyncCacheRefresher(entityMutationResponse,
                                RequestContext.get().getDifferentialEntitiesMap(),
                                RequestContext.get().getTraceId());
            asyncCacheRefresher.start();

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public void recordRefresh(EntityMutationResponse entityMutationResponse, boolean isImport) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("recordRefresh");
        try {
            if (isImport || RequestContext.get().isPoliciesBootstrappingInProgress()) {
                LOG.warn("recordRefresh: Cache refresh will be skipped");
                return;
            }

            if (!isOnDemandAuthzCacheRefreshEnabled) {
                LOG.warn("recordRefresh: Skipping as On-demand cache refresh is not enabled");
                return;
            }

            asyncCacheRefresher = new AsyncCacheRefresher(entityMutationResponse,
                    RequestContext.get().getDifferentialEntitiesMap(),
                    RequestContext.get().getTraceId());


        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void onComplete(boolean isSuccess) {
        if (isSuccess && asyncCacheRefresher != null) {
            /*try {
                LOG.info("waiting...");
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            asyncCacheRefresher.start();
        }
    }

    class AsyncCacheRefresher extends Thread {
        private String traceId;
        private EntityMutationResponse entityMutationResponse;
        private Map<String, AtlasEntity> differentialEntitiesMap;

        public AsyncCacheRefresher (EntityMutationResponse entityMutationResponse,
                                    Map<String, AtlasEntity> differentialEntitiesMap,
                                    String traceId) {
            this.traceId = traceId;
            this.entityMutationResponse = entityMutationResponse;
            this.differentialEntitiesMap = differentialEntitiesMap;
        }

        @Override
        public void run() {
            boolean refreshPolicies = false;
            boolean refreshRoles = false;

            if (entityMutationResponse == null) {
                LOG.warn("entityMutationResponse was emtpy");
                return;
            }

            if (CollectionUtils.isNotEmpty(entityMutationResponse.getCreatedEntities())) {
                for (AtlasEntityHeader entityHeader : entityMutationResponse.getCreatedEntities()) {
                    LOG.info("Updated {}", entityHeader.getTypeName());
                    if (CONNECTION_ENTITY_TYPE.equals(entityHeader.getTypeName()) || COLLECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        refreshPolicies = true;
                        refreshRoles = true;

                    } else if (PERSONA_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        refreshRoles = true;

                    } else if (POLICY_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        String policyCategory = (String) entityHeader.getAttribute(ATTR_POLICY_CATEGORY);
                        String subCategory = (String) entityHeader.getAttribute(ATTR_POLICY_SUB_CATEGORY);

                        if (POLICY_CATEGORY_PERSONA.equals(policyCategory) &&
                                (POLICY_SUB_CATEGORY_METADATA.equals(subCategory) || POLICY_SUB_CATEGORY_DATA.equals(subCategory))) {
                            refreshPolicies = true;
                        }

                        if (POLICY_CATEGORY_PURPOSE.equals(policyCategory) || "metadata".equals(subCategory)) {
                            refreshPolicies = true;
                            refreshRoles = true;
                        }
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(entityMutationResponse.getUpdatedEntities())) {
                for (AtlasEntityHeader entityHeader : entityMutationResponse.getUpdatedEntities()) {
                    AtlasEntity diffEntity = differentialEntitiesMap.get(entityHeader.getGuid());

                    if (diffEntity != null && MapUtils.isNotEmpty(diffEntity.getAttributes())) {
                        Set<String> updatedAttrs = diffEntity.getAttributes().keySet();
                        Collection<String> attrsToRefreshCache;

                        if (CONNECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            attrsToRefreshCache = CollectionUtils.intersection(updatedAttrs, CONNECTION_ATTRS);
                            if (CollectionUtils.isNotEmpty(attrsToRefreshCache)) {
                                refreshRoles = true;
                            }

                        } else if (COLLECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            attrsToRefreshCache = CollectionUtils.intersection(updatedAttrs, COLLECTION_ATTRS);
                            if (CollectionUtils.isNotEmpty(attrsToRefreshCache)) {
                                refreshRoles = true;
                            }

                        } else if (PERSONA_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            attrsToRefreshCache = CollectionUtils.intersection(updatedAttrs, PERSONA_ATTRS_ROLES);
                            if (CollectionUtils.isNotEmpty(attrsToRefreshCache)) {
                                refreshRoles = true;
                            }

                            attrsToRefreshCache = CollectionUtils.intersection(updatedAttrs, PERSONA_ATTRS_POLICIES);
                            if (CollectionUtils.isNotEmpty(attrsToRefreshCache)) {
                                refreshPolicies = true;
                            }

                        } else if (PURPOSE_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            attrsToRefreshCache = CollectionUtils.intersection(updatedAttrs, PURPOSE_ATTRS);
                            if (CollectionUtils.isNotEmpty(attrsToRefreshCache)) {
                                refreshPolicies = true;
                            }

                        } else if (POLICY_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            refreshPolicies = true;
                        }
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(entityMutationResponse.getDeletedEntities())) {
                for (AtlasEntityHeader entityHeader : entityMutationResponse.getDeletedEntities()) {

                    if (CONNECTION_ENTITY_TYPE.equals(entityHeader.getTypeName()) ||
                            COLLECTION_ENTITY_TYPE.equals(entityHeader.getTypeName()) ||
                            PERSONA_ENTITY_TYPE.equals(entityHeader.getTypeName()) ||
                            PURPOSE_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        refreshPolicies = true;
                        refreshRoles = true;

                    } else if (POLICY_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        String policyCategory = (String) entityHeader.getAttribute(ATTR_POLICY_CATEGORY);
                        String subCategory = (String) entityHeader.getAttribute(ATTR_POLICY_SUB_CATEGORY);

                        if (POLICY_CATEGORY_PERSONA.equals(policyCategory) &&
                                (POLICY_SUB_CATEGORY_METADATA.equals(subCategory) || POLICY_SUB_CATEGORY_DATA.equals(subCategory))) {
                            refreshPolicies = true;
                        }

                        if (POLICY_CATEGORY_PURPOSE.equals(policyCategory) && POLICY_SUB_CATEGORY_METADATA.equals(subCategory)) {
                            refreshPolicies = true;
                        }
                    }
                }
            }

            if (refreshPolicies || refreshRoles) {
                AuthzCacheRefreshInfo refreshInfo = new AuthzCacheRefreshInfo.Builder()
                        .setHardRefresh(true)
                        .setRefreshPolicies(refreshPolicies)
                        .setRefreshRoles(refreshRoles)
                        .build();

                AtlasAuthorizationUtils.refreshCache(refreshInfo);
                hostRefresher.refreshCache(refreshInfo, traceId);
            }
        }
    }
}
