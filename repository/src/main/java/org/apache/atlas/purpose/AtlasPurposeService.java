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
package org.apache.atlas.purpose;

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
import org.apache.atlas.persona.AtlasPersonaUtil;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.PURPOSE_ALREADY_EXISTS;
import static org.apache.atlas.PersonaPurposeCommonUtil.fetchRangerPoliciesByLabel;
import static org.apache.atlas.PersonaPurposeCommonUtil.getESAliasName;
import static org.apache.atlas.persona.AtlasPersonaUtil.getGroups;
import static org.apache.atlas.persona.AtlasPersonaUtil.getIsEnabled;
import static org.apache.atlas.persona.AtlasPersonaUtil.getName;
import static org.apache.atlas.persona.AtlasPersonaUtil.getUsers;
import static org.apache.atlas.purpose.AtlasPurposeUtil.*;
import static org.apache.atlas.repository.Constants.*;


@Component
public class AtlasPurposeService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPurposeService.class);

    private AtlasRangerService atlasRangerService;
    private final AtlasEntityStore entityStore;
    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;
    private final EntityDiscoveryService entityDiscoveryService;

    @Inject
    public AtlasPurposeService(AtlasRangerService atlasRangerService,
                               AtlasEntityStore entityStore,
                               AtlasTypeRegistry typeRegistry,
                               EntityDiscoveryService entityDiscoveryService,
                               ESAliasStore aliasStore,
                               EntityGraphRetriever entityRetriever,
                               AtlasGraph graph) {
        this.entityStore = entityStore;
        this.atlasRangerService = atlasRangerService;
        this.typeRegistry = typeRegistry;
        this.graph = graph;
        this.entityDiscoveryService = entityDiscoveryService;
        this.aliasStore = aliasStore;
        this.entityRetriever = entityRetriever;
    }

    public EntityMutationResponse createOrUpdatePurpose(AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        EntityMutationResponse ret = null;
        try {
            AtlasEntity purpose = entityWithExtInfo.getEntity();
            AtlasEntityWithExtInfo atlasEntityWithExtInfo = null;

            PurposeContext context = new PurposeContext(entityWithExtInfo);

            try {
                Map<String, Object> uniqueAttributes = mapOf(QUALIFIED_NAME, getQualifiedName(purpose));
                atlasEntityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(new AtlasObjectId(purpose.getGuid(), purpose.getTypeName(), uniqueAttributes));
            } catch (AtlasBaseException abe) {
                if (abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_GUID_NOT_FOUND &&
                        abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                    throw abe;
                }
            }

            if (atlasEntityWithExtInfo == null) {
                ret = createPurpose(context, entityWithExtInfo);
            } else {
                ret = updatePurpose(context, atlasEntityWithExtInfo);
            }

        } catch (AtlasBaseException abe) {
            //TODO: handle exception
            LOG.error("Failed to create perpose");
            throw abe;
        }
        return ret;
    }

    private EntityMutationResponse createPurpose(PurposeContext context, AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        EntityMutationResponse ret = null;
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurpose");
        LOG.info("Creating Purpose");

        context.setCreateNewPurpose(true);

        validateUniquenessByName(entityDiscoveryService, getName(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);
        validateUniquenessByTags(entityDiscoveryService, getTags(entityWithExtInfo.getEntity()), PURPOSE_ENTITY_TYPE);

        //unique qualifiedName for Purpose
        String tenantId = getTenantId(context.getPurpose());
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "tenant";
        }
        entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entityWithExtInfo.getEntity().setAttribute("enabled", true);

        ret = entityStore.createOrUpdateForImportNoCommit(new AtlasEntityStream(context.getPurpose()));

        aliasStore.createAlias(context);
        graph.commit();

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private EntityMutationResponse updatePurpose(PurposeContext context, AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurpose");
        EntityMutationResponse ret;
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
            validateUniquenessByName(entityDiscoveryService, getName(purpose), PURPOSE_ENTITY_TYPE);
        }

        //check tags update
        // if yes: check tags for uniqueness
        if (!CollectionUtils.isEqualCollection(getTags(purpose), getTags(existingPurposeEntity))) {
            needPolicyUpdate = true;
            validateUniquenessByTags(entityDiscoveryService, getTags(purpose), PURPOSE_ENTITY_TYPE);
        }

        if (needPolicyUpdate) {
            updatePurposePoliciesTag(context, existingPurposeEntity);
            aliasStore.updateAlias(context);
        }

        ret = entityStore.createOrUpdate(new AtlasEntityStream(purpose), false);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private void disablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("disablePurpose");

        cleanRangerPolicies(existingPurposeWithExtInfo.getEntity());

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void enablePurpose(AtlasEntityWithExtInfo existingPurposeWithExtInfo) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("enablePurpose");

        //TODO

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void deletePurpose(String purposeGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurpose");
        AtlasEntity.AtlasEntityWithExtInfo purposeExtInfo = entityRetriever.toAtlasEntityWithExtInfo(purposeGuid);
        AtlasEntity purpose = purposeExtInfo.getEntity();

        if(!purpose.getTypeName().equals(PURPOSE_ENTITY_TYPE)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type {}", PURPOSE_ENTITY_TYPE);
        }

        if(!purpose.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Purpose with guid {} is already deleted/purged", purposeGuid);
            return;
        }

        cleanRangerPolicies(purpose);

        aliasStore.deleteAlias(getESAliasName(purpose));

        entityStore.deleteById(purposeGuid);
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

    public void deletePurposePolicy(String purposePolicyGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePurposePolicy");
        AtlasEntity purposePolicy = entityRetriever.toAtlasEntity(purposePolicyGuid);

        if(!PURPOSE_POLICY_TYPES.contains(purposePolicy.getTypeName())) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + StringUtils.join(PURPOSE_POLICY_TYPES, ","));
        }

        if(!purposePolicy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Purpose policy with guid {} is already deleted/purged", purposePolicyGuid);
            return;
        }

        AtlasEntity.AtlasEntityWithExtInfo purposeExtInfo = entityRetriever.toAtlasEntityWithExtInfo(getPurposeGuid(purposePolicy));

        PurposeContext context = new PurposeContext(purposeExtInfo, purposePolicy);
        context.setDeletePurposePolicy(true);

        updatePurposePolicy(context, purposePolicyToRangerPolicy(context));

        List<String> actions = getActions(purposePolicy);
        if (actions.contains(ACCESS_ENTITY_READ)) {
            aliasStore.updateAlias(context);
        }

        entityStore.deleteById(purposePolicyGuid);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public EntityMutationResponse createOrUpdatePurposePolicy(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException  {
        EntityMutationResponse ret = null;
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createOrUpdatePurposePolicy");
        PurposeContext context = new PurposeContext();

        AtlasEntity purposePolicy = entityWithExtInfo.getEntity();
        validatePurposePolicyRequest(purposePolicy);

        AtlasEntity.AtlasEntityWithExtInfo existingPurposePolicy = null;

        if (AtlasTypeUtil.isAssignedGuid(purposePolicy.getGuid())) {
            existingPurposePolicy = entityRetriever.toAtlasEntityWithExtInfo(purposePolicy.getGuid());
            if (existingPurposePolicy != null) {
                context.setCreateNewPurposePolicy(false);
                context.setExistingPurposePolicy(existingPurposePolicy.getEntity());
            } else {
                context.setCreateNewPurposePolicy(true);
            }

        } else {
            context.setCreateNewPurposePolicy(true);
        }

        String purposeGuid = getPurposeGuid(purposePolicy);
        AtlasEntity.AtlasEntityWithExtInfo purposeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(purposeGuid);

        if (!AtlasEntity.Status.ACTIVE.equals(purposeWithExtInfo.getEntity().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        if (context.isCreateNewPurposePolicy()) {
            purposePolicy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getQualifiedName(purposeWithExtInfo.getEntity()), getUUID()));
        } else {
            purposePolicy.setAttribute(QUALIFIED_NAME, getQualifiedName(existingPurposePolicy.getEntity()));
        }

        context.setPurposeExtInfo(purposeWithExtInfo);
        context.setPurposePolicy(purposePolicy);
        context.setAllowPolicy(getIsAllow(purposePolicy));
        context.setAllowPolicyUpdate();

        //verify Unique name across current Persona's policies
        verifyUniqueNameForPurposePolicy(context, getName(purposePolicy), purposeWithExtInfo);

        //create/update Atlas entity for policy (no commit)
        ret = entityStore.createOrUpdateForImportNoCommit(new AtlasEntityStream(purposePolicy));

        if (context.isCreateNewPurposePolicy()) {
            if (CollectionUtils.isNotEmpty(ret.getCreatedEntities())) {
                AtlasEntityHeader createdPurposePolicyHeader = ret.getCreatedEntities().get(0);

                purposePolicy.setGuid(createdPurposePolicyHeader.getGuid());
            } else {
                throw new AtlasBaseException("Failed to create Atlas Entity");
            }

        } else if (CollectionUtils.isEmpty(ret.getUpdatedEntities())) {
            throw new AtlasBaseException("Failed to update Atlas Entity");
        }

        purposeWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(purposeGuid);
        context.setPurposeExtInfo(purposeWithExtInfo);

        createOrUpdatePurposePolicy(context);

        aliasStore.updateAlias(context);

        graph.commit();

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private RangerPolicy createOrUpdatePurposePolicy(PurposeContext context) throws AtlasBaseException {
        //convert persona policy into Ranger policies
        RangerPolicy provisionalRangerPolicy = purposePolicyToRangerPolicy(context);
        LOG.info(AtlasType.toJson(provisionalRangerPolicy));

        if (context.isCreateNewPurposePolicy()) {
            return createPurposePolicy(context, provisionalRangerPolicy);
        } else {
            return updatePurposePolicy(context, provisionalRangerPolicy);
        }
    }

    private RangerPolicy createPurposePolicy(PurposeContext context, RangerPolicy provisionalPolicy) throws AtlasBaseException {
        RangerPolicy ret = null;
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPurposePolicy");

        updatePurposePolicy(context, provisionalPolicy);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private RangerPolicy updatePurposePolicy(PurposeContext context, RangerPolicy provisionalPolicy) throws AtlasBaseException {
        RangerPolicy ret;
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePurposePolicy");
        RangerPolicy rangerPolicy = null;

        if (context.getExistingPurposePolicy() != null && !AtlasEntity.Status.ACTIVE.equals(context.getExistingPurposePolicy().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Entity not Active");
        }

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                "tag",
                String.valueOf(provisionalPolicy.getPolicyType()),
                getPurposeLabel(context.getPurpose().getGuid()));

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
                rangerPolicy.getPolicyLabels().addAll(getLabelsForPurposePolicy(context.getPurpose().getGuid(), context.getPurposePolicy().getGuid()));
            }

            ret = atlasRangerService.updateRangerPolicy(rangerPolicy);

            LOG.info("Updated Ranger Policy with ID {}", ret.getId());
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
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

    private RangerPolicy purposePolicyToRangerPolicy(PurposeContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("purposePolicyToRangerPolicy");
        RangerPolicy rangerPolicy;

        AtlasEntity purpose = context.getPurpose();

        List<String> tags = getTags(purpose);
        if (CollectionUtils.isEmpty(tags)) {
            throw new AtlasBaseException("Tags list is empty");
        }

        rangerPolicy = getRangerPolicy(context);

        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put(RESOURCE_TAG, new RangerPolicy.RangerPolicyResource(tags, false, false));
        rangerPolicy.setResources(resources);

        if (context.isMetadataPolicy()) {
            metadataPolicyToRangerPolicy(context, rangerPolicy);
        } else if (context.isDataPolicy()) {
            dataPolicyToRangerPolicy(context, rangerPolicy);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return rangerPolicy;
    }

    private void metadataPolicyToRangerPolicy(PurposeContext context, RangerPolicy rangerPolicy) throws AtlasBaseException {
        rangerPolicy.setName("purpose-tag-" + UUID.randomUUID());

        for (AtlasEntity metadataPolicy : getMetadataPolicies(context.getPurposeExtInfo())) {
            if (context.isDeletePurposePolicy() && metadataPolicy.getGuid().equals(context.getPurposePolicy().getGuid())) {
                continue;
            }

            List<String> actions = getActions(metadataPolicy);

            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
            for (String action: actions) {
                if (action.equals(LINK_ASSET_ACTION)) {
                    accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_ADD_REL)));
                    accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_UPDATE_REL)));
                    accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(ACCESS_REMOVE_REL)));
                } else {
                    accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType(action)));
                }
            }

            setPolicyItem(metadataPolicy, rangerPolicy, accesses, null);

            rangerPolicy.getPolicyLabels().add(getPurposePolicyLabel(metadataPolicy.getGuid()));
        }
    }

    private void dataPolicyToRangerPolicy(PurposeContext context, RangerPolicy rangerPolicy) throws AtlasBaseException {
        rangerPolicy.setName("purpose-tag-" + UUID.randomUUID());

        for (AtlasEntity dataPolicy : getDataPolicies(context.getPurposeExtInfo())) {
            if (context.isDeletePurposePolicy() && dataPolicy.getGuid().equals(context.getPurposePolicy().getGuid())) {
                continue;
            }

            List<String> actions = getActions(dataPolicy);

            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
            for (String action : actions) {
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(formatAccessType("heka", action)));
            }

            if (context.isDataMaskPolicy()) {
                rangerPolicy.setName("purpose-tag-mask-" + UUID.randomUUID());
                String maskType = getDataPolicyMaskType(dataPolicy);
                RangerPolicyItemDataMaskInfo maskInfo = new RangerPolicyItemDataMaskInfo(formatMaskType(maskType), null, null);
                setPolicyItem(dataPolicy, rangerPolicy, accesses, maskInfo);
            } else {
                setPolicyItem(dataPolicy, rangerPolicy, accesses, null);
            }

            rangerPolicy.getPolicyLabels().add(getPurposePolicyLabel(dataPolicy.getGuid()));
        }
    }

    private void setPolicyItem(AtlasEntity purposePolicy, RangerPolicy rangerPolicy,
                               List<RangerPolicyItemAccess> accesses, RangerPolicyItemDataMaskInfo maskInfo) {

        List<String> users = getUsers(purposePolicy);
        List<String> groups = getGroups(purposePolicy);
        if (getIsAllUsers(purposePolicy)) {
            users = null;
            groups = Collections.singletonList("public");
        }

        if (maskInfo != null) {
            RangerPolicy.RangerDataMaskPolicyItem policyItem = new RangerDataMaskPolicyItem(accesses, maskInfo, users, groups, null, null, false);
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

    private RangerPolicy getRangerPolicy(PurposeContext context){
        RangerPolicy rangerPolicy = new RangerPolicy();
        AtlasEntity purpose = context.getPurpose();

        rangerPolicy.setPolicyLabels(Arrays.asList(getPurposeLabel(purpose.getGuid()), "type:purpose"));

        rangerPolicy.setPolicyType(context.isDataMaskPolicy() ? 1 : 0);
        rangerPolicy.setServiceType("tag"); //TODO: disable & check
        rangerPolicy.setService("default_atlan"); //TODO: read from property config

        return rangerPolicy;
    }

    private void verifyUniqueNameForPurposePolicy(PurposeContext context, String newPolicyName,
                                                  AtlasEntity.AtlasEntityWithExtInfo purposeWithExtInfo) throws AtlasBaseException {

        if (!context.isCreateNewPurposePolicy() && !getName(context.getExistingPurposePolicy()).equals(getName(context.getPurpose()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();

        List<AtlasEntity> policies = getPurposeAllPolicies(purposeWithExtInfo);
        if (CollectionUtils.isNotEmpty(policies)) {
            policies.forEach(x -> policyNames.add(getName(x)));
        }

        if (policyNames.contains(newPolicyName)) {
            throw new AtlasBaseException(PURPOSE_ALREADY_EXISTS, newPolicyName);
        }
    }

    private void validatePurposePolicyRequest(AtlasEntity policy) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(getActions(policy))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for policy policy");
        }

        if (CollectionUtils.isEmpty(getUsers(policy)) && CollectionUtils.isEmpty(getGroups(policy)) && !getIsAllUsers(policy)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide users/groups OR select All users for policy policy");
        }
    }
}
