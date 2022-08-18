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

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.accesscontrol.persona.PersonaContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.ranger.client.RangerClientHelper;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.accesscontrol.AccessControlUtil.getDescription;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getName;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getQualifiedName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.*;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getGroupsAsRangerRole;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getPersonaRoleId;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getUsersAsRangerRole;


@Component
public class AtlasRangerService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRangerService.class);

    private final AtlasEntityStore entityStore;
    private final RangerClientHelper rangerClientHelper;

    @Inject
    public AtlasRangerService(AtlasEntityStore entityStore, RangerClientHelper rangerClientHelper) {
        this.entityStore = entityStore;
        this.rangerClientHelper = rangerClientHelper;
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

            ret = RangerClientHelper.createRole(rangerRole);

            LOG.info("Created: Ranger Role");

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to create Ranger role due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to create Ranger role: " + e.getMessage());
        }
        return ret;
    }

    public RangerRole getRangerRole(String roleName) throws AtlasBaseException {
        RangerRoleList roles;
        RangerRole ret;

        try {
            roles = RangerClientHelper.getRole(roleName);

            Optional<RangerRole> opt = roles.getList().stream().filter(x -> roleName.equals(x.getName())).findFirst();

            if (opt.isPresent()) {
                ret = opt.get();
            } else {
                throw new AtlasBaseException("Role for connection not found");
            }

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to get Ranger role due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to get Ranger role: " + e.getMessage());
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

            ret = RangerClientHelper.updateRole(rangerRole);

            LOG.info("Updated: Ranger Role");

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to update Ranger role due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to update Ranger role: " + e.getMessage());
        }

        return ret;
    }

    public void deleteRangerRole(long roleId) throws AtlasBaseException {
        try {
            RangerClientHelper.deleteRole(roleId);

            LOG.info("Deleted: Ranger Role {}", roleId);

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to delete Ranger role due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to delete Ranger role: " + e.getMessage());
        }
    }

    public RangerPolicy createRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        RangerPolicy ret = null;
        try {

            LOG.info("creating on Ranger \n{}\n", AtlasType.toJson(rangerPolicy));
            ret = RangerClientHelper.createPolicy(rangerPolicy);

            LOG.info("Created: Ranger Policy {}", ret.getId());

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to create Ranger policy due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to create Ranger policy: " + e.getMessage());
        }

        return ret;
    }

    public RangerPolicy updateRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        RangerPolicy ret = null;
        try {
            LOG.info("updating on Ranger \n{}\n", AtlasType.toJson(rangerPolicy));
            ret = RangerClientHelper.updatePolicy(rangerPolicy);

            LOG.info("Updated: Ranger Policy {}", ret.getId());

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to update Ranger policy due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to update Ranger policy: " + e.getMessage());
        }

        return ret;
    }

    public List<RangerPolicy> getPoliciesByResources(Map<String, String> resources,
                                                        Map<String, String> attributes) throws AtlasBaseException {
        List<RangerPolicy> ret = null;

        try {
            RangerPolicyList list = RangerClientHelper.searchPoliciesByResources(resources, attributes);

            if (list != null) {
                ret = list.getPolicies();
            }

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to searchPoliciesByResources due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to searchPoliciesByResources: " + e.getMessage());
        }

        return ret;
    }

    public List<RangerPolicy> getPoliciesByLabel(Map<String, String> attributes) throws AtlasBaseException {
        List<RangerPolicy> ret = null;

        try {
            RangerPolicyList list = RangerClientHelper.getPoliciesByLabels(attributes);

            if (list != null) {
                ret = list.getPolicies();
            }
        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to getPoliciesByLabel due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to getPoliciesByLabel: " + e.getMessage());
        }

        return ret;
    }

    public void deleteRangerPolicy(RangerPolicy rangerPolicy) throws AtlasBaseException {
        try {
            RangerClientHelper.deletePolicy(rangerPolicy.getId());

            LOG.info("Deleted: Ranger Policy {}", rangerPolicy.getId());

        } catch (AtlasServiceException e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to delete Ranger policy due to Ranger issue: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new AtlasBaseException("Failed to delete Ranger policy: " + e.getMessage());
        }
    }
}
