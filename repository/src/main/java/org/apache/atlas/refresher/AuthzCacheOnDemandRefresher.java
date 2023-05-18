package org.apache.atlas.refresher;


import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.EnableConditional;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graph.AuthzCacheRefresher;
import org.apache.atlas.repository.graph.TypeCacheRefresher;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.HashSet;
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
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ACTIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_GROUPS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_IS_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_PRIORITY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ROLES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SUB_CATEGORY;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_TYPE;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_USERS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PURPOSE_CLASSIFICATIONS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_CATEGORY_PURPOSE;

@Component
@EnableConditional(property = "atlas.authorizer.enable.action.based.cache.refresh")
public class AuthzCacheOnDemandRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzCacheOnDemandRefresher.class);

    private final AtlasTypeRegistry typeRegistry;
    private final AuthzCacheRefresher hostRefresher;

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

    private static final Set<String> PERSONA_ATTRS = new HashSet<String>(){{
        add(ATTR_ACCESS_CONTROL_ENABLED);
        add(ATTR_POLICY_USERS);
        add(ATTR_POLICY_GROUPS);
    }};

    private static final Set<String> PURPOSE_ATTRS = new HashSet<String>(){{
        add(ATTR_ACCESS_CONTROL_ENABLED);
        add(ATTR_PURPOSE_CLASSIFICATIONS);
    }};

    @Inject
    public AuthzCacheOnDemandRefresher(AtlasTypeRegistry typeRegistry, AuthzCacheRefresher hostRefresher) {
        this.typeRegistry = typeRegistry;
        this.hostRefresher = hostRefresher;
    }

    public void refreshCacheIfNeeded(EntityMutationResponse entityMutationResponse, boolean isImport) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("refreshCacheIfNeeded");

        try {
            if (isImport || RequestContext.get().isPoliciesBootstrappingInProgress()) {
                LOG.warn("Cache refresh will be skipped");
                return;
            }

            boolean refreshPolicies = false;
            boolean refreshRoles = false;

            if (CollectionUtils.isNotEmpty(entityMutationResponse.getCreatedEntities())) {
                for (AtlasEntityHeader entityHeader : entityMutationResponse.getCreatedEntities()) {
                    if (CONNECTION_ENTITY_TYPE.equals(entityHeader.getTypeName()) || COLLECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        refreshPolicies = true;
                        refreshRoles = true;

                    } else if (PERSONA_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        refreshRoles = true;

                    } else if (POLICY_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                        String policyCategory = (String) entityHeader.getAttribute(ATTR_POLICY_CATEGORY);
                        String subCategory = (String) entityHeader.getAttribute(ATTR_POLICY_SUB_CATEGORY);

                        if (POLICY_CATEGORY_PERSONA.equals(policyCategory) &&
                                ("metadata".equals(subCategory) || "data".equals(subCategory))) {
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
                    AtlasEntity diffEntity = RequestContext.get().getDifferentialEntity(entityHeader.getGuid());

                    if (diffEntity != null && MapUtils.isNotEmpty(diffEntity.getAttributes())) {
                        Set<String> updatedAttrs = diffEntity.getAttributes().keySet();

                        if (CONNECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            updatedAttrs.retainAll(CONNECTION_ATTRS);
                            if (CollectionUtils.isNotEmpty(updatedAttrs)) {
                                refreshRoles = true;
                            }

                        } else if (COLLECTION_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            updatedAttrs.retainAll(COLLECTION_ATTRS);
                            if (CollectionUtils.isNotEmpty(updatedAttrs)) {
                                refreshRoles = true;
                            }

                        } else if (PERSONA_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            updatedAttrs.retainAll(PERSONA_ATTRS);
                            if (CollectionUtils.isNotEmpty(updatedAttrs)) {
                                refreshRoles = true;
                            }

                        } else if (PURPOSE_ENTITY_TYPE.equals(entityHeader.getTypeName())) {
                            updatedAttrs.retainAll(PURPOSE_ATTRS);
                            if (CollectionUtils.isNotEmpty(updatedAttrs)) {
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
                                ("metadata".equals(subCategory) || "data".equals(subCategory))) {
                            refreshPolicies = true;
                        }

                        if (POLICY_CATEGORY_PURPOSE.equals(policyCategory) && "metadata".equals(subCategory)) {
                            refreshPolicies = true;
                        }
                    }
                }
            }

            if (refreshPolicies || refreshRoles) {
                AtlasAuthorizationUtils.refreshCache(refreshPolicies, refreshRoles, false);
                hostRefresher.refreshCache(refreshPolicies, refreshRoles, false);
                LOG.info("refreshCacheIfNeeded: hosts refreshed!");
            }

        } finally {
            RequestContext.get().endMetricRecord(recorder);
            LOG.info("refreshCacheIfNeeded: done!");
        }
    }
}
