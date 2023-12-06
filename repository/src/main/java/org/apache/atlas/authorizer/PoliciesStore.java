package org.apache.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PoliciesStore {

    private static final Logger LOG = LoggerFactory.getLogger(PoliciesStore.class);

    private static List<RangerPolicy> resourcePolicies;
    private static List<RangerPolicy> tagPolicies;
    private static List<RangerPolicy> abacPolicies;

    private static PoliciesStore policiesStore;

    public static PoliciesStore getInstance() {
        synchronized (PoliciesStore.class) {
            if (policiesStore == null) {
                policiesStore = new PoliciesStore();
            }
            return policiesStore;
        }
    }

    public void setResourcePolicies(List<RangerPolicy> resourcePolicies) {
        this.resourcePolicies = resourcePolicies;
    }

    private static List<RangerPolicy> getResourcePolicies() {
        return resourcePolicies;
    }

    public void setTagPolicies(List<RangerPolicy> tagPolicies) {
        this.tagPolicies = tagPolicies;
    }

    private static List<RangerPolicy> getTagPolicies() {
        return tagPolicies;
    }

    public void setAbacPolicies(List<RangerPolicy> abacPolicies) {
        this.abacPolicies = abacPolicies;
    }

    private static List<RangerPolicy> getAbacPolicies() {
        return abacPolicies;
    }

    public static List<RangerPolicy> getRelevantPolicies(String persona, String purpose, String serviceName, List<String> actions, String policyType) {
        String policyQualifiedNamePrefix = null;
        if (persona != null && !persona.isEmpty()) {
            policyQualifiedNamePrefix = persona;
        } else if (purpose != null && !purpose.isEmpty()) {
            policyQualifiedNamePrefix = purpose;
        }

        String user = AuthorizerCommon.getCurrentUserName();
        LOG.info("Getting relevant policies for user: {}", user);

        RangerUserStore userStore = UsersStore.getUserStore();
        List<String> groups = UsersStore.getGroupsForUser(user, userStore);

        RangerRoles allRoles = UsersStore.getAllRoles();
        List<String> roles = UsersStore.getRolesForUser(user, allRoles);
        roles.addAll(UsersStore.getNestedRolesForUser(roles, allRoles));

        List<RangerPolicy> policies = new ArrayList<>();
        if ("atlas".equals(serviceName)) {
            policies = getResourcePolicies();
        } else if ("atlas_tag".equals(serviceName)) {
            policies =getTagPolicies();
        } else if ("atlas_abac".equals(serviceName)) {
            policies = getAbacPolicies();
        }

        if (CollectionUtils.isNotEmpty(policies)) {
            policies = getFilteredPoliciesForQualifiedName(policies, policyQualifiedNamePrefix);
            policies = getFilteredPoliciesForUser(policies, user, groups, roles, policyType);
            policies = getFilteredPoliciesForActions(policies, actions, policyType);
        }
        return policies;

    }

    static List<RangerPolicy> getFilteredPoliciesForQualifiedName(List<RangerPolicy> policies, String qualifiedNamePrefix) {
        if (qualifiedNamePrefix != null && !qualifiedNamePrefix.isEmpty()) {
            List<RangerPolicy> filteredPolicies = new ArrayList<>();
            for(RangerPolicy policy : policies) {
                if (policy.getName().startsWith(qualifiedNamePrefix)) {
                    filteredPolicies.add(policy);
                }
            }
            return filteredPolicies;
        }
        return policies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForActions(List<RangerPolicy> policies, List<String> actions, String type) {
        List<RangerPolicy> filteredPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;
            if (AuthorizerCommon.POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (AuthorizerCommon.POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
            if (policyItem != null) {
                List<String> policyActions = new ArrayList<>();
                if (!policyItem.getAccesses().isEmpty()) {
                    for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                        policyActions.add(access.getType());
                    }
                }
                if (AuthorizerCommon.arrayListContains(policyActions, actions)) {
                    filteredPolicies.add(policy);
                }
            }
        }
        return filteredPolicies;
    }

    private static List<RangerPolicy> getFilteredPoliciesForUser(List<RangerPolicy> policies, String user, List<String> groups, List<String> roles, String type) {
        List<RangerPolicy> filterPolicies = new ArrayList<>();
        for(RangerPolicy policy : policies) {
            RangerPolicy.RangerPolicyItem policyItem = null;
            if (AuthorizerCommon.POLICY_TYPE_ALLOW.equals(type) && !policy.getPolicyItems().isEmpty()) {
                policyItem = policy.getPolicyItems().get(0);
            } else if (AuthorizerCommon.POLICY_TYPE_DENY.equals(type) && !policy.getDenyPolicyItems().isEmpty()) {
                policyItem = policy.getDenyPolicyItems().get(0);
            }
            if (policyItem != null) {
                List<String> policyUsers = policyItem.getUsers();
                List<String> policyGroups = policyItem.getGroups();
                List<String> policyRoles = policyItem.getRoles();
                if (policyUsers.contains(user) || AuthorizerCommon.arrayListContains(policyGroups, groups) || AuthorizerCommon.arrayListContains(policyRoles, roles)) {
                    filterPolicies.add(policy);
                }
            }
        }
        return filterPolicies;
    }
}
