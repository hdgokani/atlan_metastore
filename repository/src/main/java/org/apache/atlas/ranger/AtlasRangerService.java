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
package org.apache.atlas.ranger;

import org.apache.atlas.RequestContext;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.ranger.client.RangerClient;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.RANGER_POLICY_FIND_FAILED;
import static org.apache.atlas.AtlasErrorCode.RANGER_POLICY_MUTATION_FAILED;
import static org.apache.atlas.AtlasErrorCode.RANGER_ROLE_MUTATION_FAILED;
import static org.apache.atlas.AtlasErrorCode.RANGER_ROLE_NOT_FOUND;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getDescription;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getGroupsAsRangerRole;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getPersonaRoleId;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getUsersAsRangerRole;


public class AtlasRangerService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRangerService.class);

    private static RangerClient client;

    static {
        client = new RangerClient();
    }

    public RangerRole createRangerRole(PersonaContext context) throws AtlasBaseException {
        RangerRole ret;

        try {
            AtlasEntity personaEntity = context.getPersona();

            RangerRole rangerRole = new RangerRole();
            rangerRole.setName(getQualifiedName(personaEntity));
            rangerRole.setUsers(getUsersAsRangerRole(personaEntity));
            rangerRole.setGroups(getGroupsAsRangerRole(personaEntity));

            rangerRole.setDescription(getDescription(personaEntity));
            if (StringUtils.isEmpty(rangerRole.getDescription())) {
                rangerRole.setDescription("For persona entity with name " + getName(personaEntity));
            }

            ret = client.createRole(rangerRole);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_ROLE_MUTATION_FAILED, "create", getQualifiedName(context.getPersona()), e.getMessage());
        }
        return ret;
    }

    public RangerRole getRangerRole(String roleName) throws AtlasBaseException {
        RangerRoleList roles;
        RangerRole ret;

        try {
            roles = client.getRole(roleName);

            Optional<RangerRole> opt = roles.getList().stream().filter(x -> roleName.equals(x.getName())).findFirst();

            if (opt.isPresent()) {
                ret = opt.get();
            } else {
                throw new AtlasBaseException(RANGER_ROLE_NOT_FOUND, roleName, String.format("Role with name %s not present", roleName));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_ROLE_NOT_FOUND, roleName, e.getMessage());
        }
        return ret;
    }

    public RangerRole updateRangerRole(PersonaContext context) throws AtlasBaseException {
        RangerRole ret;

        try {
            AtlasEntity personaEntity = context.getPersona();

            RangerRole rangerRole = new RangerRole();
            rangerRole.setName(getQualifiedName(personaEntity));
            rangerRole.setUsers(getUsersAsRangerRole(personaEntity));
            rangerRole.setGroups(getGroupsAsRangerRole(personaEntity));
            rangerRole.setId(getPersonaRoleId(personaEntity));

            rangerRole.setDescription(getDescription(personaEntity));

            ret = client.updateRole(rangerRole);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_ROLE_MUTATION_FAILED, "update", getQualifiedName(context.getPersona()), e.getMessage());
        }

        return ret;
    }

    public void deleteRangerRole(long roleId) throws AtlasBaseException {
        try {
            client.deleteRole(roleId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_ROLE_MUTATION_FAILED, "delete", "roleId " + roleId, e.getMessage());
        }
    }

    public RangerPolicy createRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        RangerPolicy ret;
        try {
            ret = client.createPolicy(rangerPolicy);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_POLICY_MUTATION_FAILED, "create", e.getMessage());
        }

        return ret;
    }

    public RangerPolicy updateRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        RangerPolicy ret;
        try {
            ret = client.updatePolicy(rangerPolicy);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_POLICY_MUTATION_FAILED, "update", e.getMessage());
        }

        return ret;
    }

    public void deleteRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        try {
            client.deletePolicyById(rangerPolicy.getId());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_POLICY_MUTATION_FAILED, "delete", e.getMessage());
        }
    }

    public List<RangerPolicy> getPoliciesByResources(Map<String, String> resources,
                                                     Map<String, String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("searchPoliciesByResources");
        List<RangerPolicy> ret = null;

        try {
            RangerPolicyList list = client.searchPoliciesByResources(resources, attributes);

            if (list != null) {
                ret = list.getPolicies();
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_POLICY_FIND_FAILED, "resources:" + resources, e.getMessage());
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    public List<RangerPolicy> getPoliciesByLabel(Map<String, String> attributes) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPoliciesByLabel");
        List<RangerPolicy> ret = null;

        try {
            RangerPolicyList list = client.getPoliciesByLabel(attributes);

            if (list != null) {
                ret = list.getPolicies();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException(RANGER_POLICY_FIND_FAILED, "labels:" + attributes, e.getMessage());
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }
}
