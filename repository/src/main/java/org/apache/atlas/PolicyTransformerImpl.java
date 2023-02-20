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

package org.apache.atlas;

import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.ranger.plugin.model.RangerPolicy;
import org.apache.atlas.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.atlas.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.atlas.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.atlas.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.atlas.ranger.plugin.model.RangerValiditySchedule;
import org.apache.atlas.ranger.plugin.util.ServicePolicies;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.util.FileUtils;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class PolicyTransformerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyTransformerImpl.class);

    @Inject
    EntityDiscoveryService discoveryService;

    public PolicyTransformerImpl() {
        //this.discoveryService = discoveryService;
    }

    //private EntityDiscoveryService discoveryService = null;
    /*@Inject
    public PolicyTransformer(EntityDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }*/


    public ServicePolicies getPolicies(String serviceName, String pluginId) {
        //TODO: return only if updated
        ServicePolicies servicePolicies = new ServicePolicies();

        try {
            servicePolicies.setServiceName("atlas");

            List<RangerPolicy> policies = getServicePolicies(serviceName);
            //servicePolicies.setPolicies(policies);

        } catch (Exception e) {
            LOG.error("ERROR in getPolicies {}: ", e.getMessage());
        }


        return servicePolicies;
    }

    private List<RangerPolicy> getServicePolicies(String serviceName) throws AtlasBaseException, IOException {
        List<RangerPolicy> servicePolicies = new ArrayList<>();

        AtlasEntityHeader service = getServiceEntity(serviceName);

        List<AtlasEntityHeader> atlasPolicies = getAtlasPolicies((String) service.getAttribute("name"));

        if (CollectionUtils.isNotEmpty(atlasPolicies)) {
            //transform policies
            servicePolicies = transformAtlasPolicies(atlasPolicies);
        }


        return servicePolicies;
    }

    private List<RangerPolicy> transformAtlasPolicies(List<AtlasEntityHeader> atlasPolicies) throws IOException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        for (AtlasEntityHeader atlasPolicy : atlasPolicies) {
            RangerPolicy rangerPolicy = new RangerPolicy();

            //rangerPolicy.setId(atlasPolicy.getGuid());
            rangerPolicy.setName((String) atlasPolicy.getAttribute("qualifiedName"));
            rangerPolicy.setServiceType((String) atlasPolicy.getAttribute("policyServiceName"));

            rangerPolicy.setConditions(getPolicyConditions(atlasPolicy));
            rangerPolicy.setValiditySchedules(getPolicyValiditySchedule(atlasPolicy));

            //GET policy Item
            setPolicyItems(rangerPolicy, atlasPolicy);

            //GET policy Resources
            setPolicyResources(atlasPolicy);

        }

        return rangerPolicies;
    }

    private void setPolicyResources(AtlasEntityHeader atlasPolicy) throws IOException {
        if ("CUSTOM".equals((String) atlasPolicy.getAttribute("policyResourceCategory"))) {
            File jsonTemplateFile = new File(getClass().getResource("PolicyCacheTransformer.json").getPath());
            String jsonTemplate = FileCopyUtils.copyToString(new FileReader(jsonTemplateFile));

            PolicyTransformerTemplate template = AtlasType.fromJson(
                    jsonTemplate,
                    PolicyTransformerTemplate.class);
            String temp = "";
        }
    }

    private void setPolicyItems(RangerPolicy rangerPolicy, AtlasEntityHeader atlasPolicy) {

        String policyType = (String) atlasPolicy.getAttribute("policyType");

        List<String> users = (List<String>) atlasPolicy.getAttribute("policyUsers");
        List<String> groups = (List<String>) atlasPolicy.getAttribute("policyGroups");
        List<String> roles = (List<String>) atlasPolicy.getAttribute("policyRoles");

        List<RangerPolicyItemAccess> accesses = new ArrayList<>();
        List<String> actions = (List<String>) atlasPolicy.getAttribute("policyActions");

        actions.forEach(action -> accesses.add(new RangerPolicyItemAccess(action)));


        //TODO: set accesses
        if ("allow".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setPolicyItems(Collections.singletonList(item));

        } else if ("deny".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setDenyPolicyItems(Collections.singletonList(item));

        } else if ("allowExceptions".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setAllowExceptions(Collections.singletonList(item));

        } else if ("denyExceptions".equals(policyType)) {
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            rangerPolicy.setDenyExceptions(Collections.singletonList(item));

        } else if ("dataMask".equals(policyType)) {
            RangerDataMaskPolicyItem item = new RangerDataMaskPolicyItem();
            item.setUsers(users);
            item.setGroups(groups);
            item.setRoles(roles);
            item.setAccesses(accesses);

            //TODO
            //String masktype =
            //RangerPolicy.RangerPolicyItemDataMaskInfo dataMaskInfo

        } else if ("rowFilter".equals(policyType)) {
            //TODO
        }

    }

    private List<RangerPolicyItemCondition> getPolicyConditions(AtlasEntityHeader atlasPolicy) {
        List<RangerPolicyItemCondition> ret = new ArrayList<>();

        List<HashMap<String, Object>> conditions = (List<HashMap<String, Object>>) atlasPolicy.getAttribute("policyConditions");


        for (HashMap<String, Object> condition : conditions) {
            RangerPolicyItemCondition rangerCondition = new RangerPolicyItemCondition();

            rangerCondition.setType((String) condition.get("policyConditionType"));
            rangerCondition.setValues((List<String>) condition.get("policyConditionValues"));

            ret.add(rangerCondition);
        }
        return ret;
    }

    private List<RangerValiditySchedule> getPolicyValiditySchedule(AtlasEntityHeader atlasPolicy) {
        List<RangerValiditySchedule> ret = new ArrayList<>();

        List<HashMap<String, String>> validitySchedules = (List<HashMap<String, String>>) atlasPolicy.getAttribute("policyValiditySchedule");


        for (HashMap<String, String> validitySchedule : validitySchedules) {
            RangerValiditySchedule rangerValiditySchedule = new RangerValiditySchedule();

            rangerValiditySchedule.setStartTime(validitySchedule.get("policyValidityScheduleStartTime"));
            rangerValiditySchedule.setEndTime(validitySchedule.get("policyValidityScheduleEndTime"));
            rangerValiditySchedule.setTimeZone(validitySchedule.get("policyValidityScheduleTimezone"));

            ret.add(rangerValiditySchedule);
        }
        return ret;
    }

    private List<AtlasEntityHeader> getAtlasPolicies(String serviceName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Set<String> attributes = new HashSet<>();
        attributes.add("name");
        attributes.add("policyCategory");
        attributes.add("policyType");
        attributes.add("policyServiceName");
        attributes.add("policyUsers");
        attributes.add("policyGroups");
        attributes.add("policyRoles");
        attributes.add("policyActions");
        attributes.add("policyResources");
        attributes.add("policyValiditySchedule");
        attributes.add("policyConditions");

        Map<String, Object> dsl = getMap("size", 0);

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(getMap("match", getMap("policyServiceName", serviceName)));
        mustClauseList.add(getMap("match", getMap("__state", Id.EntityState.ACTIVE)));

        dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(attributes);

        List<AtlasEntityHeader> ret = new ArrayList<>();

        int from = 0;
        int size = 100;

        do {
            dsl.put("from", from);
            dsl.put("size", size);

            List<AtlasEntityHeader> headers = discoveryService.directIndexSearch(indexSearchParams).getEntities();
            if (headers != null) {
                ret.addAll(headers);
            }

            from += size;

        } while (ret.size() > 0 && ret.size() % size == 0);

        return ret;
    }

    private AtlasEntityHeader getServiceEntity(String serviceName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Set<String> attributes = new HashSet<>();
        attributes.add("name");
        attributes.add("authServiceType");
        attributes.add("tagService");
        attributes.add("authServiceIsEnabled");

        Map<String, Object> dsl = getMap("size", 1);

        List<Map<String, Object>> mustClauseList = new ArrayList<>();
        mustClauseList.add(getMap("match", getMap("__typeName", "AuthService")));
        mustClauseList.add(getMap("match", getMap("name", serviceName)));

        dsl.put("query", getMap("bool", getMap("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(attributes);

        AtlasSearchResult searchResult = discoveryService.directIndexSearch(indexSearchParams);

        return searchResult.getEntities().get(0);

    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
