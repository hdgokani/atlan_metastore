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
package org.apache.atlas.accesscontrol.purpose;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.ESAliasStore;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.PURPOSE_ALREADY_EXISTS;
import static org.apache.atlas.accesscontrol.AccessControlUtil.fetchRangerPoliciesByLabel;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getESAliasName;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getIsEnabled;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.getName;
import static org.apache.atlas.accesscontrol.purpose.AtlasPurposeUtil.*;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class AtlasPurposeService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPurposeService.class);

    private final AtlasGraph graph;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;

    private AtlasRangerService atlasRangerService;

    public AtlasPurposeService(AtlasGraph graph,
                               EntityGraphRetriever entityRetriever) {
        this.atlasRangerService = new AtlasRangerService();
        this.graph = graph;
        this.entityRetriever = entityRetriever;

        this.aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    public EntityMutationResponse createPurpose(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurpose");
        EntityMutationResponse ret = null;
        LOG.info("Creating Purpose");

        AtlasEntityWithExtInfo entityWithExtInfo = context.getPurposeExtInfo();

        context.setCreateNewPurpose(true);

        validateUniquenessByName(graph, getName(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);
        validateUniquenessByTags(graph, getTags(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);

        //unique qualifiedName for Purpose
        String tenantId = getTenantId(context.getPurpose());
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "tenant";
        }
        entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, tenantId, getUUID()));
        entityWithExtInfo.getEntity().setAttribute("enabled", true);

        aliasStore.createAlias(context);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    public void updatePurpose(PurposeContext context, AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurpose");
        LOG.info("Updating Purpose");
        boolean needPolicyUpdate = false;

        AtlasEntity purpose = context.getPurpose();
        AtlasEntity existingPurposeEntity = existingPurposeWithExtInfo.getEntity();

        purpose.setAttribute(QUALIFIED_NAME, getQualifiedName(existingPurposeEntity));

        if (!AtlasEntity.Status.ACTIVE.equals(existingPurposeEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        if (getIsEnabled(existingPurposeEntity) != getIsEnabled(purpose)) {
            if (getIsEnabled(purpose)) {
                enablePurpose(existingPurposeWithExtInfo);
            } else {
                disablePurpose(existingPurposeWithExtInfo);
            }
        }

        //check name update
        // if yes: check naw name for uniqueness
        if (!getName(purpose).equals(getName(existingPurposeEntity))) {
            validateUniquenessByName(graph, getName(purpose), PURPOSE_ENTITY_TYPE);
        }

        //check tags update
        // if yes: check tags for uniqueness
        if (!CollectionUtils.isEqualCollection(getTags(purpose), getTags(existingPurposeEntity))) {
            needPolicyUpdate = true;
            validateUniquenessByTags(graph, getTags(purpose), PURPOSE_ENTITY_TYPE);
        }

        if (needPolicyUpdate && CollectionUtils.isNotEmpty(getPolicies(context.getPurposeExtInfo()))) {
            updatePurposePoliciesTag(context, existingPurposeEntity);
            aliasStore.updateAlias(context);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void disablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("disablePurpose");

        cleanRangerPolicies(existingPurposeWithExtInfo.getEntity());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void enablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("enablePurpose");

        PurposeContext context = new PurposeContext();
        context.setPurposeExtInfo(existingPurposeWithExtInfo);

        List<RangerPolicy> provisionalRangerPolicies = purposePolicyToRangerPolicy(context);
        updatePurposePolicies(context, provisionalRangerPolicies);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void deletePurpose(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurpose");
        AtlasEntity purpose = personaExtInfo.getEntity();

        if(!purpose.getTypeName().equals(PURPOSE_ENTITY_TYPE)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + PURPOSE_ENTITY_TYPE);
        }

        if(!purpose.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Purpose with guid {} is already deleted/purged", purpose.getGuid());
            return;
        }

        cleanRangerPolicies(purpose);

        aliasStore.deleteAlias(getESAliasName(purpose));

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void cleanRangerPolicies(AtlasEntity purpose) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                "tag",
                null,
                getPurposeLabel(purpose.getGuid()));

        if (CollectionUtils.isEmpty(rangerPolicies)) {
            rangerPolicies = fetchRangerPoliciesByResourcesForPurposeDelete(atlasRangerService, purpose);
        }

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            for (RangerPolicy policy : rangerPolicies) {
                atlasRangerService.deleteRangerPolicy(policy);
            }
        }
    }

    public void deletePurposePolicy(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurposePolicy");
        context.setDeletePurposePolicy(true);

        AtlasEntity purposePolicy = context.getPurposePolicy();

        if(!POLICY_ENTITY_TYPE.equals(purposePolicy.getTypeName())) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + POLICY_ENTITY_TYPE);
        }

        if(!purposePolicy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Purpose policy with guid {} is already deleted/purged", purposePolicy.getGuid());
            return;
        }

        List<RangerPolicy> provisionalRangerPolicies = purposePolicyToRangerPolicy(context);
        updatePurposePolicies(context, provisionalRangerPolicies);

        processDeletePolicy(context, provisionalRangerPolicies);

        List<String> actions = getActions(purposePolicy);
        if (actions.contains(ACCESS_ENTITY_READ)) {
            aliasStore.updateAlias(context);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processDeletePolicy(PurposeContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = null;

        if (CollectionUtils.isEmpty(provisionalRangerPolicies)) {
            rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    "tag",
                    null,
                    getPurposeLabel(context.getPurpose().getGuid()));
        } else if (provisionalRangerPolicies.size() == 1) {
            rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    "tag",
                    String.valueOf(provisionalRangerPolicies.get(0).getPolicyType() == 0 ? 1 : 0),
                    getPurposeLabel(context.getPurpose().getGuid()));
        }

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            atlasRangerService.deleteRangerPolicy(rangerPolicies.get(0));
        }
    }

    public void createPurposePolicy(PurposeContext context) throws AtlasBaseException  {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurposePolicy");

        AtlasEntity purposePolicy = context.getPurposePolicy();
        context.setCreateNewPurposePolicy(true);
        context.setAllowPolicy(getIsAllow(purposePolicy));
        context.setAllowPolicyUpdate();

        validatePurposePolicy(context);

        purposePolicy.setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, getQualifiedName(context.getPurpose()), getUUID()));

        List<RangerPolicy> provisionalRangerPolicies = purposePolicyToRangerPolicy(context);
        updatePurposePolicies(context, provisionalRangerPolicies);

        aliasStore.updateAlias(context);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void updatePurposePolicy(PurposeContext context) throws AtlasBaseException  {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurposePolicy");

        AtlasEntity purposePolicy = context.getPurposePolicy();
        context.setCreateNewPurposePolicy(false);
        context.setAllowPolicy(getIsAllow(purposePolicy));
        context.setAllowPolicyUpdate();

        validatePurposePolicy(context);

        List<RangerPolicy> provisionalRangerPolicies = purposePolicyToRangerPolicy(context);
        updatePurposePolicies(context, provisionalRangerPolicies);

        aliasStore.updateAlias(context);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void validatePurposePolicy(PurposeContext context) throws AtlasBaseException {
        validatePurposePolicyRequest(context);
        verifyUniqueNameForPurposePolicy(context);
    }

    private void updatePurposePolicies(PurposeContext context, List<RangerPolicy> provisionalPolicies) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurposePolicies");
        RangerPolicy ret;

        if (context.getExistingPurposePolicy() != null && !AtlasEntity.Status.ACTIVE.equals(context.getExistingPurposePolicy().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Entity not Active");
        }

        for (RangerPolicy provisionalPolicy : provisionalPolicies) {
            List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                    "tag",
                    String.valueOf(provisionalPolicy.getPolicyType()),
                    getPurposeLabel(context.getPurpose().getGuid()));

            RangerPolicy rangerPolicy = null;
            if (CollectionUtils.isEmpty(rangerPolicies)) {
                rangerPolicy = fetchRangerPolicyByResources(atlasRangerService,
                        "tag",
                        String.valueOf(provisionalPolicy.getPolicyType()),
                        provisionalPolicy);

            } else if (rangerPolicies.size() == 1) {
                rangerPolicy = rangerPolicies.get(0);
            } else {
                String resourcesSignature = new RangerPolicyResourceSignature(provisionalPolicy).getSignature();

                for (RangerPolicy matchedRangerPolicy : rangerPolicies) {
                    String labelMatchedPolicyResourcesSignature = new RangerPolicyResourceSignature(matchedRangerPolicy).getSignature();

                    if (resourcesSignature.equals(labelMatchedPolicyResourcesSignature)) {
                        rangerPolicy = matchedRangerPolicy;
                    }
                }
            }

            if (rangerPolicy == null) {
                ret = atlasRangerService.createRangerPolicy(provisionalPolicy);
            } else {

                rangerPolicy.setPolicyItems(provisionalPolicy.getPolicyItems());
                rangerPolicy.setDenyPolicyItems(provisionalPolicy.getDenyPolicyItems());
                rangerPolicy.setDataMaskPolicyItems(provisionalPolicy.getDataMaskPolicyItems());

                if (context.isDeletePurposePolicy()) {
                    rangerPolicy.getPolicyLabels().remove(getPurposePolicyLabel(context.getPurposePolicy().getGuid()));
                } else {
                    rangerPolicy.getPolicyLabels().addAll(provisionalPolicy.getPolicyLabels());
                }

                ret = atlasRangerService.updateRangerPolicy(rangerPolicy);

                LOG.info("Updated Ranger Policy with ID {}", ret.getId());
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updatePurposePoliciesTag(PurposeContext context, AtlasEntity existingPurposeEntity) throws AtlasBaseException {
        RangerPolicy accessPolicy = null;
        RangerPolicy maskingPolicy = null;

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                "tag",
                null,
                getPurposeLabel(context.getPurpose().getGuid()));

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            List<String> tags = getTags(existingPurposeEntity);

            for (RangerPolicy rangerPolicy : rangerPolicies) {
                List<String> rangerPolicyTags = rangerPolicy.getResources().get("tag").getValues();

                if (CollectionUtils.isEqualCollection(tags, rangerPolicyTags)) {

                    if (Integer.valueOf("0").equals(rangerPolicy.getPolicyType())) {
                        accessPolicy = rangerPolicy;
                    }

                    if (Integer.valueOf("1").equals(rangerPolicy.getPolicyType())) {
                        maskingPolicy = rangerPolicy;
                    }
                }
            }
        }

        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put(RESOURCE_TAG, new RangerPolicy.RangerPolicyResource(getTags(context.getPurpose()), false, false));

        if (maskingPolicy != null) {
            maskingPolicy.setResources(resources);
            atlasRangerService.updateRangerPolicy(maskingPolicy);
        }

        if (accessPolicy != null) {
            accessPolicy.setResources(resources);
            atlasRangerService.updateRangerPolicy(accessPolicy);
        }
    }

    private List<RangerPolicy> purposePolicyToRangerPolicy(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("purposePolicyToRangerPolicy");

        List<RangerPolicy> ret = new ArrayList<>();
        RangerPolicy rangerAccessPolicy, rangerMaskPolicy;

        AtlasEntity purpose = context.getPurpose();

        List<String> tags = getTags(purpose);
        if (CollectionUtils.isEmpty(tags)) {
            throw new AtlasBaseException("Tags list is empty");
        }

        rangerAccessPolicy = getRangerPolicy(context, 0);
        rangerMaskPolicy = getRangerPolicy(context, 1);

        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put(RESOURCE_TAG, new RangerPolicy.RangerPolicyResource(tags, false, false));

        rangerAccessPolicy.setResources(resources);
        rangerMaskPolicy.setResources(resources);

        policyToRangerPolicy(context, rangerAccessPolicy, rangerMaskPolicy);

        if (CollectionUtils.isNotEmpty(rangerAccessPolicy.getPolicyItems()) || CollectionUtils.isNotEmpty(rangerAccessPolicy.getDenyPolicyItems())) {
            ret.add(rangerAccessPolicy);
        }

        if (CollectionUtils.isNotEmpty(rangerMaskPolicy.getDataMaskPolicyItems())) {
            ret.add(rangerMaskPolicy);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private void policyToRangerPolicy(PurposeContext context, RangerPolicy rangerAccessPolicy, RangerPolicy rangerMaskPolicy) {
        rangerAccessPolicy.setName("purpose-tag-" + UUID.randomUUID());

        for (AtlasEntity policy : getPolicies(context.getPurposeExtInfo())) {
            if (context.isDeletePurposePolicy() && policy.getGuid().equals(context.getPurposePolicy().getGuid())) {
                continue;
            }

            List<String> actions = getActions(policy);
            List<RangerPolicyItemAccess> accesses = new ArrayList<>();

            if (isDataPolicy(policy)) {
                for (String action : actions) {
                    accesses.add(new RangerPolicyItemAccess(formatAccessType("heka", action)));
                }

                if (isDataMaskPolicy(policy)) {
                    rangerMaskPolicy.setName("purpose-tag-mask-" + UUID.randomUUID());
                    String maskType = getDataPolicyMaskType(policy);
                    RangerPolicyItemDataMaskInfo maskInfo = new RangerPolicyItemDataMaskInfo(formatMaskType(maskType), null, null);
                    setPolicyItem(policy, rangerMaskPolicy, accesses, maskInfo);
                    rangerMaskPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));

                } else {
                    setPolicyItem(policy, rangerAccessPolicy, accesses, null);
                    rangerAccessPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));
                }
            } else {
                for (String action : actions) {
                    if (action.equals(LINK_ASSET_ACTION)) {
                        accesses.add(new RangerPolicyItemAccess(formatAccessType(ACCESS_ADD_REL)));
                        accesses.add(new RangerPolicyItemAccess(formatAccessType(ACCESS_UPDATE_REL)));
                        accesses.add(new RangerPolicyItemAccess(formatAccessType(ACCESS_REMOVE_REL)));
                    } else {
                        accesses.add(new RangerPolicyItemAccess(formatAccessType(action)));
                    }
                }
                setPolicyItem(policy, rangerAccessPolicy, accesses, null);
                rangerAccessPolicy.getPolicyLabels().add(getPurposePolicyLabel(policy.getGuid()));
            }
        }
    }

    private void setPolicyItem(AtlasEntity purposePolicy, RangerPolicy rangerPolicy,
                               List<RangerPolicyItemAccess> accesses, RangerPolicyItemDataMaskInfo maskInfo) {

        List<String> users = getPolicyUsers(purposePolicy);
        List<String> groups = getPolicyGroups(purposePolicy);
        if (getIsAllUsers(purposePolicy)) {
            users = null;
            groups = Collections.singletonList("public");
        }

        if (maskInfo != null) {
            RangerDataMaskPolicyItem policyItem = new RangerDataMaskPolicyItem(accesses, maskInfo, users, groups, null, null, false);
            rangerPolicy.getDataMaskPolicyItems().add(policyItem);

        } else {
            RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, users, groups, null, null, false);

            if (getIsAllow(purposePolicy)) {
                rangerPolicy.getPolicyItems().addAll(Collections.singletonList(policyItem));
            } else {
                rangerPolicy.getDenyPolicyItems().addAll(Collections.singletonList(policyItem));
            }
        }
    }

    private RangerPolicy getRangerPolicy(PurposeContext context, int policyType){
        RangerPolicy rangerPolicy = new RangerPolicy();
        AtlasEntity purpose = context.getPurpose();

        rangerPolicy.setPolicyLabels(Arrays.asList(getPurposeLabel(purpose.getGuid()), "type:purpose"));

        rangerPolicy.setPolicyType(policyType);
        rangerPolicy.setServiceType("tag"); //TODO: disable & check
        rangerPolicy.setService("default_atlan"); //TODO: read from property config

        return rangerPolicy;
    }

    private void verifyUniqueNameForPurposePolicy(PurposeContext context) throws AtlasBaseException {

        if (!context.isCreateNewPurposePolicy() && !getName(context.getExistingPurposePolicy()).equals(getName(context.getPurpose()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();
        String newPolicyName = getName(context.getPurposePolicy());

        List<AtlasEntity> policies = getPolicies(context.getPurposeExtInfo());

        if (CollectionUtils.isNotEmpty(policies)) {
            if (context.isCreateNewPurposePolicy()) {
                policies = policies.stream()
                        .filter(x -> !x.getGuid().equals(context.getPurposePolicy().getGuid()))
                        .collect(Collectors.toList());
            }
        }

        policies.forEach(x -> policyNames.add(getName(x)));

        if (policyNames.contains(newPolicyName)) {
            throw new AtlasBaseException(PURPOSE_ALREADY_EXISTS, newPolicyName);
        }
    }

    private void validatePurposePolicyRequest(PurposeContext context) throws AtlasBaseException {
        if (!AtlasEntity.Status.ACTIVE.equals(context.getPurpose().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        if (CollectionUtils.isEmpty(getActions(context.getPurposePolicy()))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for policy policy");
        }

        if (CollectionUtils.isEmpty(getPolicyUsers(context.getPurposePolicy())) &&
                CollectionUtils.isEmpty(getPolicyGroups(context.getPurposePolicy())) && !getIsAllUsers(context.getPurposePolicy())) {

            throw new AtlasBaseException(BAD_REQUEST, "Please provide users/groups OR select All users for policy policy");
        }
    }
}
