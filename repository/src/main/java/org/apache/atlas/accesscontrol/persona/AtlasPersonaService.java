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
package org.apache.atlas.accesscontrol.persona;

import org.apache.atlas.ESAliasStore;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE;
import static org.apache.atlas.AtlasConfiguration.RANGER_HEKA_SERVICE_TYPE;
import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.PERSONA_ALREADY_EXISTS;
import static org.apache.atlas.AtlasErrorCode.RANGER_DUPLICATE_POLICY;
import static org.apache.atlas.AtlasErrorCode.UNAUTHORIZED_CONNECTION_ADMIN;
import static org.apache.atlas.accesscontrol.AccessControlUtil.*;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.*;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


public class AtlasPersonaService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPersonaService.class);

    private final AtlasGraph graph;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;

    private static AtlasRangerService atlasRangerService = null;

    public AtlasPersonaService(AtlasGraph graph, EntityGraphRetriever entityRetriever) {

        this.graph = graph;
        this.entityRetriever = entityRetriever;
        this.aliasStore = new ESAliasStore(graph, entityRetriever);

        atlasRangerService = new AtlasRangerService();
    }

    public void createPersona(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPersona");
        LOG.info("Creating Persona");
        EntityMutationResponse ret;
        AtlasEntityWithExtInfo entityWithExtInfo = context.getPersonaExtInfo();
        context.setCreateNewPersona(true);

        validateUniquenessByName(graph, getName(entityWithExtInfo.getEntity()), PERSONA_ENTITY_TYPE);

        String tenantId = getTenantId(context.getPersona());
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "tenant";
        }
        entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entityWithExtInfo.getEntity().setAttribute("enabled", true);

        RangerRole rangerRole = atlasRangerService.createRangerRole(context);
        context.getPersona().getAttributes().put("rangerRoleId", rangerRole.getId());

        aliasStore.createAlias(context);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void updatePersona(PersonaContext context, AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePersona");
        LOG.info("Updating Persona");

        AtlasEntity persona = context.getPersona();

        AtlasEntity existingPersonaEntity = existingPersonaWithExtInfo.getEntity();

        if (!AtlasEntity.Status.ACTIVE.equals(existingPersonaEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona not Active");
        }

        if (getPersonaRoleId(persona) != getPersonaRoleId(existingPersonaEntity)) {
            throw new AtlasBaseException(ATTRIBUTE_UPDATE_NOT_SUPPORTED, PERSONA_ENTITY_TYPE, "rangerRoleId");
        }

        if (getIsEnabled(existingPersonaEntity) != getIsEnabled(persona)) {
            if (getIsEnabled(context.getPersona())) {
                enablePersona(existingPersonaWithExtInfo);
            } else {
                disablePersona(existingPersonaWithExtInfo);
            }
        }

        if (!getName(persona).equals(getName(existingPersonaEntity))) {
            validateUniquenessByName(graph, getName(persona), PERSONA_ENTITY_TYPE);
        }

        atlasRangerService.updateRangerRole(context);

        aliasStore.updateAlias(context);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void deletePersona(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePersona");
        AtlasEntity persona = personaExtInfo.getEntity();

        if(!persona.getTypeName().equals(PERSONA_ENTITY_TYPE)) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + PERSONA_ENTITY_TYPE);
        }

        if(!persona.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Persona with guid {} is already deleted/purged", persona.getGuid());
            return;
        }

        cleanRoleFromAllRangerPolicies(personaExtInfo);

        atlasRangerService.deleteRangerRole(getPersonaRoleId(persona));

        aliasStore.deleteAlias(getESAliasName(persona));

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void cleanRoleFromAllRangerPolicies(AtlasEntityWithExtInfo personaExtInfo) throws AtlasBaseException {
        AtlasEntity persona = personaExtInfo.getEntity();

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                null,
                null,
                getPersonaLabel(persona.getGuid()));

        List<String> allPolicyGuids = getPolicies(personaExtInfo).stream().map(x -> getPersonaPolicyLabel(x.getGuid())).collect(Collectors.toList());

        cleanRoleFromExistingPolicies(personaExtInfo.getEntity(), rangerPolicies, allPolicyGuids);
    }

    private void enablePersona(AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        List<AtlasEntity> personaPolicies = getPolicies(existingPersonaWithExtInfo);

        for (AtlasEntity personaPolicy : personaPolicies) {
            PersonaContext contextItr = new PersonaContext(existingPersonaWithExtInfo, personaPolicy);
            contextItr.setAllowPolicy(getIsAllow(personaPolicy));
            contextItr.setAllowPolicyUpdate();
            contextItr.setCreateNewPersonaPolicy(true);

            List<RangerPolicy> provisionalRangerPolicies = personaPolicyToRangerPolicies(contextItr, getActions(personaPolicy));

            createPersonaPolicy(contextItr, provisionalRangerPolicies);
        }
    }

    private void disablePersona(AtlasEntityWithExtInfo existingPersonaWithExtInfo) throws AtlasBaseException {
        cleanRoleFromAllRangerPolicies(existingPersonaWithExtInfo);
    }

    public void createPersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPersonaPolicy");

        AtlasEntityWithExtInfo personaWithExtInfo = context.getPersonaExtInfo();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        context.setCreateNewPersonaPolicy(true);
        context.setAllowPolicy(getIsAllow(personaPolicy));
        context.setAllowPolicyUpdate();

        personaPolicy.setAttribute(QUALIFIED_NAME, String.format(POLICY_QN_FORMAT, getQualifiedName(personaWithExtInfo.getEntity()), getUUID()));

        validatePersonaPolicy(context);

        List<RangerPolicy> provisionalRangerPolicies = personaPolicyToRangerPolicies(context, getActions(context.getPersonaPolicy()));

        if (CollectionUtils.isNotEmpty(provisionalRangerPolicies)) {
            createPersonaPolicy(context, provisionalRangerPolicies);
        } else {
            throw new AtlasBaseException("provisionalRangerPolicies could not be empty");
        }

        if (context.isMetadataPolicy() || context.isGlossaryPolicy()) {
            aliasStore.updateAlias(context);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void updatePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePersonaPolicy");
        Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap = new HashMap<>();

        AtlasEntity personaPolicy = context.getPersonaPolicy();

        context.setAllowPolicy(getIsAllow(personaPolicy));
        context.setAllowPolicyUpdate();

        validatePersonaPolicyUpdate(context);

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                null,
                getPersonaPolicyLabel(personaPolicy.getGuid()));

        List<RangerPolicy> provisionalRangerPolicies = personaPolicyToRangerPolicies(context, getActions(context.getPersonaPolicy()));

        if (context.isUpdateIsAllow() || isAssetUpdate(context) || isDataPolicyTypeUpdate(context)) {
            //remove role from existing policies & create new Ranger policies
            List<String> removePolicyGuids = Collections.singletonList(getPersonaPolicyLabel(personaPolicy.getGuid()));
            cleanRoleFromExistingPolicies(context.getPersona(), rangerPolicies, removePolicyGuids);

            for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
                provisionalToRangerPoliciesMap.put(provisionalRangerPolicy, null);
            }

        } else {
            provisionalToRangerPoliciesMap = mapPolicies(context, provisionalRangerPolicies, rangerPolicies);
        }

        if (MapUtils.isNotEmpty(provisionalToRangerPoliciesMap)) {
            processUpdatePolicies(context, provisionalToRangerPoliciesMap);
        }

        processActionsRemoval(context);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void validatePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        validatePersonaPolicyRequest(context);
        validateConnectionAdmin(context);
        verifyUniqueNameForPersonaPolicy(context);
        verifyUniquePersonaPolicy(context);
    }

    private void validatePersonaPolicyUpdate(PersonaContext context) throws AtlasBaseException {
        validatePersonaPolicy(context);

        if (!getPolicyType(context.getPersonaPolicy()).equals(getPolicyType(context.getExistingPersonaPolicy()))) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Policy type change not Allowed");
        }

        if (!AtlasEntity.Status.ACTIVE.equals(context.getExistingPersonaPolicy().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Entity not Active");
        }
    }


    private void validateConnectionAdmin(PersonaContext context) throws AtlasBaseException {
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        if (isMetadataPolicy(personaPolicy) || isDataPolicy(personaPolicy)) {

            String connectionGuid = getConnectionId(personaPolicy);
            AtlasEntity connection = entityRetriever.toAtlasEntity(connectionGuid);

            if (connection != null) {
                context.setConnection(connection);
            }

            String connectionRoleName = "connection_admins_" + connectionGuid;
            RangerRole connectionAdminRole = atlasRangerService.getRangerRole(connectionRoleName);

            List<String> users = connectionAdminRole.getUsers().stream().map(x -> x.getName()).collect(Collectors.toList());
            if (!users.contains(AtlasAuthorizationUtils.getCurrentUserName())) {
                throw new AtlasBaseException(UNAUTHORIZED_CONNECTION_ADMIN, AtlasAuthorizationUtils.getCurrentUserName(), connectionGuid);
            }
        }
    }

    private List<RangerPolicy> createPersonaPolicy(PersonaContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> ret = new ArrayList<>();

        submitCallablesAndWaitToFinish("createPersonaPolicyWorker",
                provisionalRangerPolicies.stream().map(x -> new CreateRangerPolicyWorker(context, x)).collect(Collectors.toList()));

        return ret;
    }

    private boolean isAssetUpdate(PersonaContext context) {
        return !CollectionUtils.isEqualCollection(getAssets(context.getExistingPersonaPolicy()), getAssets(context.getPersonaPolicy()));
    }

    private boolean isDataPolicyTypeUpdate(PersonaContext context) {
        if (!isDataPolicy(context.getPersonaPolicy())) {
            return false;
        }

        String existingMask = getDataPolicyMaskType(context.getExistingPersonaPolicy());
        existingMask = existingMask == null ? "" : existingMask;

        String newMask = getDataPolicyMaskType(context.getPersonaPolicy());

        return !existingMask.equals(newMask) && (StringUtils.isEmpty(existingMask) || StringUtils.isEmpty(newMask));
    }

    public void deletePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePersonaPolicy");
        context.setDeletePersonaPolicy(true);

        AtlasEntity personaPolicy = context.getPersonaPolicy();
        AtlasEntityWithExtInfo personaExtInfo = context .getPersonaExtInfo();

        if(!POLICY_ENTITY_TYPE.equals(personaPolicy.getTypeName())) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + POLICY_ENTITY_TYPE);
        }

        if(!personaPolicy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
            LOG.info("Persona policy with guid {} is already deleted/purged", personaPolicy.getGuid());
            return;
        }

        List<RangerPolicy> rangerPolicies = fetchRangerPoliciesByLabel(atlasRangerService,
                context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                getPersonaPolicyLabel(personaPolicy.getGuid()));

        String role = getRoleName(personaExtInfo.getEntity());

        for (RangerPolicy rangerPolicy : rangerPolicies) {
            boolean needUpdate = false;

            if (context.isDataMaskPolicy()) {
                List<RangerDataMaskPolicyItem> policyItems  = rangerPolicy.getDataMaskPolicyItems();

                for (RangerPolicyItem policyItem : new ArrayList<>(policyItems)) {
                    if (policyItem.getRoles().remove(role)) {
                        needUpdate = true;
                        if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                            policyItems.remove(policyItem);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(rangerPolicy.getDataMaskPolicyItems())) {
                    atlasRangerService.deleteRangerPolicy(rangerPolicy);
                    needUpdate = false;
                }
            } else {
                List<RangerPolicyItem> policyItems = getIsAllow(personaPolicy) ?
                        rangerPolicy.getPolicyItems() :
                        rangerPolicy.getDenyPolicyItems();

                for (RangerPolicyItem policyItem : new ArrayList<>(policyItems)) {
                    if (policyItem.getRoles().remove(role)) {
                        needUpdate = true;
                        if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                            policyItems.remove(policyItem);
                        }
                    }
                }

                if (CollectionUtils.isEmpty(rangerPolicy.getPolicyItems()) && CollectionUtils.isEmpty(rangerPolicy.getDenyPolicyItems())) {
                    atlasRangerService.deleteRangerPolicy(rangerPolicy);
                    needUpdate = false;
                }
            }

            if (needUpdate) {
                rangerPolicy.getPolicyLabels().remove(getPersonaPolicyLabel(personaPolicy.getGuid()));
                rangerPolicy.getPolicyLabels().remove(getPersonaLabel(personaExtInfo.getEntity().getGuid()));

                long policyLabelCount = rangerPolicy.getPolicyLabels().stream().filter(x -> x.startsWith(LABEL_PREFIX_PERSONA)).count();
                if (policyLabelCount == 0) {
                    rangerPolicy.getPolicyLabels().remove(LABEL_TYPE_PERSONA);
                }

                atlasRangerService.updateRangerPolicy(rangerPolicy);
            }
        }

        List<String> actions = getActions(personaPolicy);
        if (actions.contains(ACCESS_ENTITY_READ)) {
            aliasStore.updateAlias(context);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void cleanRoleFromExistingPolicies(AtlasEntity persona, List<RangerPolicy> rangerPolicies,
                                               List<String> removePolicyGuids) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("cleanRoleFromExistingPolicies");
        LOG.info("clean role from existing {} policies", rangerPolicies.size());

        submitCallablesAndWaitToFinish("cleanRoleWorker",
                rangerPolicies.stream().map(x -> new CleanRoleWorker(persona, x, removePolicyGuids)).collect(Collectors.toList()));

        RequestContext.get().endMetricRecord(recorder);
    }

    /*
     *
     * This method removes action that does not have any other action left of its type
     * e.g. consider entity action type -> entity-read,entity-create,entity-update,entity-delete
     *      There were only one entity action in policy say entity-read,
     *      removing entity-read while updating policy will call this method
     *
     *
     * @Param existingRangerPolicies found by label policy search
     *
     *
     *    check if resource match is found in existingRangerPolicies
     *        if yes, remove access from policy Item
     *            check if no access remaining, remove item if true
     *                 check if no policy item remaining, delete Ranger policy if true
     *        if not, search by resources
     *            if found, remove access from policy Item
     *                check if no access remaining, remove item if true
     *                     check if no policy item remaining, delete Ranger policy if true
     *
     * */
    private void processActionsRemoval(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processActionsRemoval");
        List<String> existingActions = getActions(context.getExistingPersonaPolicy());
        List<String> updatedActions = getActions(context.getPersonaPolicy());

        List<String> removedActions = existingActions.stream()
                .filter(x -> !updatedActions.contains(x))
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(removedActions)) {
            String role = getRoleName(context.getPersona());
            List<RangerPolicy> provisionalPoliciesForDelete = personaPolicyToRangerPolicies(context, removedActions);

            for (RangerPolicy provisionalPolicyToDelete : provisionalPoliciesForDelete) {
                String provisionalPolicySignature = new RangerPolicyResourceSignature(provisionalPolicyToDelete).getSignature();

                for (RangerPolicy rangerPolicy : context.getExcessExistingRangerPolicies()) {

                    String existingRangerPolicySignature = new RangerPolicyResourceSignature(rangerPolicy).getSignature();

                    if (provisionalPolicySignature.equals(existingRangerPolicySignature)) {

                        List<RangerPolicyItem> policyItemsToUpdate;

                        if (context.isAllowPolicy()) {
                            policyItemsToUpdate = rangerPolicy.getPolicyItems();
                        } else {
                            policyItemsToUpdate = rangerPolicy.getDenyPolicyItems();
                        }

                        List<RangerPolicyItem> tempPoliciesItems = new ArrayList<>(policyItemsToUpdate);

                        for (int j = 0; j < tempPoliciesItems.size(); j++) {
                            RangerPolicyItem policyItem = tempPoliciesItems.get(j);
                            boolean update = false;

                            if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                                if (CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getUsers())) {
                                    policyItemsToUpdate.remove(policyItem);

                                    if (CollectionUtils.isEmpty(policyItemsToUpdate)) {
                                        atlasRangerService.deleteRangerPolicy(rangerPolicy);
                                    } else {
                                        update = true;
                                    }
                                } else {
                                    policyItemsToUpdate.get(j).getRoles().remove(role);
                                    update = true;
                                }
                            }

                            if (update) {
                                atlasRangerService.updateRangerPolicy(rangerPolicy);
                            }
                        }

                    } else {
                        //Rare Case, if label removed from Ranger policy manually
                    }
                }
            }
        }
        RequestContext.get().endMetricRecord(recorder);
    }

    private void processUpdatePolicies(PersonaContext context,
                                       Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap) throws AtlasBaseException {
        if (MapUtils.isEmpty(provisionalToRangerPoliciesMap)) {
            throw new AtlasBaseException("Policies map is empty");
        }
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processUpdatePolicies");

        submitCallablesAndWaitToFinish("updateRangerPolicyWorker",
                provisionalToRangerPoliciesMap.entrySet().stream().map(x -> new UpdateRangerPolicyWorker(context, x.getValue(), x.getKey())).collect(Collectors.toList()));

        RequestContext.get().endMetricRecord(recorder);
    }

    /*
     * @Param provisionalRangerPolicies -> Policies transformed from AtlasPersonaPolicy to Ranger policy
     * @Param existingRangerPolicies -> Policies found by label search
     * */
    private Map<RangerPolicy, RangerPolicy> mapPolicies(PersonaContext context,
                                                        List<RangerPolicy> provisionalRangerPolicies,
                                                        List<RangerPolicy> existingRangerPolicies) {

        Map<RangerPolicy, RangerPolicy> ret = new HashMap<>();

        for (RangerPolicy existingRangerPolicy: existingRangerPolicies) {
            boolean mapped = false;
            String existingRangerPolicySignature = new RangerPolicyResourceSignature(existingRangerPolicy).getSignature();
            int existingPolicyType = existingRangerPolicy.getPolicyType();

            for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
                String provisionalRangerPolicySignature = new RangerPolicyResourceSignature(provisionalRangerPolicy).getSignature();
                int provisionalPolicyType = provisionalRangerPolicy.getPolicyType();

                if (existingRangerPolicySignature.equals(provisionalRangerPolicySignature) && existingPolicyType == provisionalPolicyType) {
                    ret.put(provisionalRangerPolicy, existingRangerPolicy);
                    mapped = true;
                    break;
                }
            }

            if (!mapped) {
                //excess Ranger policy for persona policy
                context.addExcessExistingRangerPolicy(existingRangerPolicy);
            }
        }

        for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
            if (!ret.containsKey(provisionalRangerPolicy)) {
                ret.put(provisionalRangerPolicy, null);
            }
        }

        return ret;
    }

    private void verifyUniquePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        List<AtlasEntity> policies = null;

        if (context.isMetadataPolicy()) {
            policies = getMetadataPolicies(context.getPersonaExtInfo());
        } else if (context.isGlossaryPolicy()) {
            policies = getGlossaryPolicies(context.getPersonaExtInfo());
        }  else if (context.isDataPolicy()) {
            policies = getDataPolicies(context.getPersonaExtInfo());
        }
        verifyUniqueAssetsForPolicy(context, policies, context.getPersonaPolicy().getGuid());
    }

    private void verifyUniqueAssetsForPolicy(PersonaContext context, List<AtlasEntity> policies, String guidToExclude) throws AtlasBaseException {
        AtlasEntity newPersonaPolicy = context.getPersonaPolicy();
        List<String> newPersonaPolicyAssets = getAssets(newPersonaPolicy);

        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity policy : policies) {
                List<String> assets = getAssets(policy);

                if (!StringUtils.equals(guidToExclude, policy.getGuid()) && context.isDataMaskPolicy() == isDataMaskPolicy(policy) && assets.equals(newPersonaPolicyAssets)) {
                    throw new AtlasBaseException(RANGER_DUPLICATE_POLICY, getName(policy), policy.getGuid());
                }
            }
        }
    }

    /*
     * This method will convert a persona policy into multiple Ranger
     * policies based on actions in persona policy
     *
     * @param personaPolicy persona policy object
     * @returns List<RangerPolicy> list of Ranger policies corresponding to provided Persona policy
     * */
    private List<RangerPolicy> personaPolicyToRangerPolicies(PersonaContext context, List<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        if (context.isMetadataPolicy()) {
            rangerPolicies = metadataPolicyToRangerPolicy(context, new HashSet<>(actions));
        } else if (context.isGlossaryPolicy()) {
            rangerPolicies = glossaryPolicyToRangerPolicy(context, new HashSet<>(actions));
        } else if (context.isDataPolicy()) {
            rangerPolicies = dataPolicyToRangerPolicy(context, new HashSet<>(actions));
        }

        return rangerPolicies;
    }

    private List<RangerPolicy> glossaryPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Glossary qualified name list is empty");
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>();
        assets.forEach(x -> rangerPolicyItemAssets.add("*" + x + "*"));

        String roleName = getRoleName(persona);

        for (String action : new HashSet<>(actions)) {
            if (!actions.contains(action)) {
                continue;
            }

            Map<String, RangerPolicyResource> resources = new HashMap<>();
            List<RangerPolicyItemAccess> accesses = new ArrayList<>();
            String policyName = "";

            if (ENTITY_ACTIONS.contains(action)) {
                if (actions.contains(ACCESS_ENTITY_CREATE)) {
                    rangerPolicies.addAll(glossaryPolicyToRangerPolicy(context, new HashSet<String>() {{
                        add(GLOSSARY_TERM_RELATIONSHIP);
                    }}));
                }

                policyName = "Glossary-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        actions.remove(entityAction);
                        accesses.add(new RangerPolicyItemAccess(entityAction));
                    }
                }
            }

            if (action.equals(GLOSSARY_TERM_RELATIONSHIP)) {
                policyName = "Glossary-term-relationship-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicyResource("*"));

                accesses.add(new RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(GLOSSARY_TERM_RELATIONSHIP);
            }

            if (LABEL_ACTIONS.contains(action)) {
                policyName = "Glossary-labels-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                resources.put(RESOURCE_ENTITY_LABEL, new RangerPolicyResource("*"));

                for (String labelAction : LABEL_ACTIONS) {
                    if (actions.contains(labelAction)) {
                        actions.remove(labelAction);
                        accesses.add(new RangerPolicyItemAccess(labelAction));
                    }
                }
            }

            if (CLASSIFICATION_ACTIONS.contains(action)) {
                policyName = "Glossary-classification-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                resources.put(RESOURCE_CLASS, new RangerPolicyResource("*"));

                for (String tagAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(tagAction)) {
                        actions.remove(tagAction);
                        accesses.add(new RangerPolicyItemAccess(tagAction));
                    }
                }
            }

            if (BM_ACTION.equals(action)) {
                policyName = "Glossary-entity-business-metadata-" + UUID.randomUUID();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));

                resources.put(RESOURCE_BM, new RangerPolicyResource("*"));

                accesses.add(new RangerPolicyItemAccess(BM_ACTION));
                actions.remove(BM_ACTION);
            }

            if (LINK_ASSET_ACTION.equals(action)) {
                policyName = "Glossary-relationship-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicyResource("*"));


                accesses.add(new RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(LINK_ASSET_ACTION);

                rangerPolicies.addAll(glossaryPolicyToRangerPolicy(context, new HashSet<String>() {{ add(RELATED_TERMS); }}));
            }

            if (action.equals(RELATED_TERMS)) {
                policyName = "Glossary-related-terms-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicyResource("*"));


                accesses.add(new RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_UPDATE_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(RELATED_TERMS);
            }

            if (MapUtils.isNotEmpty(resources)) {
                RangerPolicy rangerPolicy = getRangerPolicy(context);

                rangerPolicy.setName(policyName);
                rangerPolicy.setResources(resources);

                RangerPolicyItem policyItem = getPolicyItem(accesses, roleName);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private List<RangerPolicy> metadataPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Assets list is empty");
        }

        boolean isConnection = false;
        if (assets.size() == 1 && context.getConnection() != null) {
            String connectionQualifiedName = getQualifiedName(context.getConnection());
            if (assets.get(0).equals(connectionQualifiedName)) {
                isConnection = true;
            }
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>(assets);
        assets.forEach(x -> rangerPolicyItemAssets.add(x + "/*"));

        String roleName = getRoleName(persona);

        for (String action : new HashSet<>(actions)) {
            if (!actions.contains(action)) {
                continue;
            }

            Map<String, RangerPolicyResource> resources = new HashMap<>();
            List<RangerPolicyItemAccess> accesses = new ArrayList<>();
            String policyName = "";

            if (ENTITY_ACTIONS.contains(action)) {
                policyName = "CRUD-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        actions.remove(entityAction);
                        accesses.add(new RangerPolicyItemAccess(entityAction));
                    }
                }
            }

            if (CLASSIFICATION_ACTIONS.contains(action)) {
                policyName = "classification-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                for (String tagAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(tagAction)) {
                        actions.remove(tagAction);
                        accesses.add(new RangerPolicyItemAccess(tagAction));
                    }
                }
            }

            if (BM_ACTION.equals(action)) {
                policyName = "entity-business-metadata-" + UUID.randomUUID();

                if (isConnection) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_BM, new RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));

                accesses.add(new RangerPolicyItemAccess(BM_ACTION));
                actions.remove(BM_ACTION);
            }

            if (TERM_ACTIONS.contains(action)) {
                policyName = "terms-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicyResource(ATLAS_GLOSSARY_TERM_ENTITY_TYPE));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicyResource("*"));


                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicyResource("*"));

                for (String termAction : TERM_ACTIONS) {
                    if (actions.contains(termAction)) {
                        actions.remove(termAction);
                        accesses.add(new RangerPolicyItemAccess("add-terms".equals(termAction) ? ACCESS_ADD_REL : ACCESS_REMOVE_REL));
                    }
                }
            }

            if (LINK_ASSET_ACTION.equals(action)) {
                policyName = "link-assets-" + UUID.randomUUID();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicyResource(Arrays.asList("Catalog", "Connection", "Dataset", "Infrastructure", "Process", "ProcessExecution", "Namespace"), false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicyResource("*"));

                accesses.add(new RangerPolicyItemAccess(ACCESS_ADD_REL));
                accesses.add(new RangerPolicyItemAccess(ACCESS_REMOVE_REL));
                actions.remove(LINK_ASSET_ACTION);
            }

            if (MapUtils.isNotEmpty(resources)) {
                RangerPolicy rangerPolicy = getRangerPolicy(context);

                rangerPolicy.setName(policyName);
                rangerPolicy.setResources(resources);

                RangerPolicyItem policyItem = getPolicyItem(accesses, roleName);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private List<RangerPolicy> dataPolicyToRangerPolicy(PersonaContext context, Set<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getAssets(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Assets list is empty");
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>(assets);
        assets.forEach(x -> rangerPolicyItemAssets.add(x + "/*"));

        for (String action : new HashSet<>(actions)) {
            RangerPolicy rangerPolicy = getRangerPolicy(context);

            if (SELECT_ACTION.contains(action)) {
                rangerPolicy.setName("dataPolicy-" + UUID.randomUUID());

                Map<String, RangerPolicyResource> resources = new HashMap<>();
                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicyResource(rangerPolicyItemAssets, false, false));
                rangerPolicy.setResources(resources);

                List<RangerPolicyItemAccess> accesses = Collections.singletonList(new RangerPolicyItemAccess(SELECT_ACTION));

                if (context.isDataMaskPolicy()) {
                    rangerPolicy.setName("dataPolicy-mask" + UUID.randomUUID());
                    RangerPolicyItemDataMaskInfo maskInfo = new RangerPolicyItemDataMaskInfo(getDataPolicyMaskType(personaPolicy), null, null);

                    RangerDataMaskPolicyItem policyItem = new RangerDataMaskPolicyItem(accesses, maskInfo,  null,
                            null, Arrays.asList(getRoleName(persona)), null, false);

                    rangerPolicy.setDataMaskPolicyItems(Arrays.asList(policyItem));

                } else {
                    RangerPolicyItem policyItem = getPolicyItem(accesses, getRoleName(persona));

                    if (context.isAllowPolicy()) {
                        rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                    } else {
                        rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                    }
                }
            }

            if (MapUtils.isNotEmpty(rangerPolicy.getResources())) {
                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private RangerPolicyItem getPolicyItem(List<RangerPolicyItemAccess> accesses, String roleName) {
        return new RangerPolicyItem(accesses, null,
                null, Arrays.asList(roleName), null, false);
    }

    private RangerPolicy getRangerPolicy(PersonaContext context){
        RangerPolicy rangerPolicy = new RangerPolicy();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        rangerPolicy.setPolicyLabels(getLabelsForPersonaPolicy(persona.getGuid(), personaPolicy.getGuid()));

        rangerPolicy.setPolicyType(context.isDataMaskPolicy() ? 1 : 0);

        rangerPolicy.setService(context.isDataPolicy() ? "heka" : "atlas");

        return rangerPolicy;
    }

    private void validatePersonaPolicyRequest(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("validatePersonaPolicyRequest");

        AtlasEntity personaPolicy = context.getPersonaPolicy();

        if (!AtlasEntity.Status.ACTIVE.equals(context.getPersonaExtInfo().getEntity().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona is not Active");
        }

        if (CollectionUtils.isEmpty(getActions(personaPolicy))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for persona policy");
        }

        if (isMetadataPolicy(personaPolicy)) {
            if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
            }

            if (StringUtils.isEmpty(getConnectionId(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide connectionGuid for persona policy");
            }
        }

        if (isGlossaryPolicy(personaPolicy)) {
            if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
            }
        }

        if (isDataPolicy(personaPolicy)) {
            if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
            }

            if (StringUtils.isEmpty(getConnectionId(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide connectionGuid for persona policy");
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }


    private void verifyUniqueNameForPersonaPolicy(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("verifyUniqueNameForPersonaPolicy");

        if (!context.isCreateNewPersonaPolicy() && !getName(context.getExistingPersonaPolicy()).equals(getName(context.getPersona()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();

        List<AtlasEntity> personaPolicies = getPolicies(context.getPersonaExtInfo());
        if (CollectionUtils.isNotEmpty(personaPolicies)) {
            if (context.isCreateNewPersonaPolicy()) {
                personaPolicies = personaPolicies.stream()
                        .filter(x -> !x.getGuid().equals(context.getPersonaPolicy().getGuid()))
                        .collect(Collectors.toList());
            }
        }

        personaPolicies.forEach(x -> policyNames.add(getName(x)));

        String newPolicyName = getName(context.getPersonaPolicy());
        if (policyNames.contains(newPolicyName)) {
            throw new AtlasBaseException(PERSONA_ALREADY_EXISTS, newPolicyName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private static class CreateRangerPolicyWorker implements Callable<RangerPolicy> {
        private static final Logger LOG = LoggerFactory.getLogger(CreateRangerPolicyWorker.class);

        private PersonaContext context;
        private RangerPolicy provisionalPolicy;

        public CreateRangerPolicyWorker(PersonaContext context, RangerPolicy provisionalPolicy) {
            this.context = context;
            this.provisionalPolicy = provisionalPolicy;
        }

        @Override
        public RangerPolicy call() {
            RangerPolicy ret = null;
            LOG.info("Starting CreateRangerPolicyWorker");

            RangerPolicy rangerPolicy = null;
            try {
                //check if there is existing Ranger policy of current provisional Ranger policy
                rangerPolicy = fetchRangerPolicyByResources(atlasRangerService,
                        context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                        context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                        provisionalPolicy);

                if (rangerPolicy == null) {
                    ret = atlasRangerService.createRangerPolicy(provisionalPolicy);
                } else {
                    if (context.isDataMaskPolicy()) {
                        rangerPolicy.getDataMaskPolicyItems().add(provisionalPolicy.getDataMaskPolicyItems().get(0));
                    } else if (context.isAllowPolicy()) {
                        rangerPolicy.getPolicyItems().add(provisionalPolicy.getPolicyItems().get(0));
                    } else {
                        rangerPolicy.getDenyPolicyItems().add(provisionalPolicy.getDenyPolicyItems().get(0));
                    }

                    List<String> labels = rangerPolicy.getPolicyLabels();
                    labels.addAll(getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid()));
                    rangerPolicy.setPolicyLabels(labels);

                    ret = atlasRangerService.updateRangerPolicy(rangerPolicy);
                }
            } catch (AtlasBaseException e) {
                LOG.error("Failed to create Ranger policies: {}", e.getMessage());
            } finally {
                LOG.info("End CreateRangerPolicyWorker");
            }

            return ret;
        }
    }

    private static class CleanRoleWorker implements Callable<RangerPolicy> {
        private static final Logger LOG = LoggerFactory.getLogger(CleanRoleWorker.class);

        private RangerPolicy rangerPolicy;
        private AtlasEntity persona;
        private List<String> removePolicyGuids;

        public CleanRoleWorker(AtlasEntity persona, RangerPolicy rangerPolicy, List<String> removePolicyGuids) {
            this.rangerPolicy = rangerPolicy;
            this.persona = persona;
            this.removePolicyGuids = removePolicyGuids;
        }

        @Override
        public RangerPolicy call() {
            RangerPolicy ret = null;
            LOG.info("Starting CleanRoleWorker");

            try {
                boolean deletePolicy = false;
                String role = getRoleName(persona);

                if (rangerPolicy.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS) {
                    deletePolicy = cleanRoleFromAccessPolicy(role, rangerPolicy);
                } else {
                    deletePolicy = cleanRoleFromMaskingPolicy(role, rangerPolicy);
                }

                if (deletePolicy) {
                    atlasRangerService.deleteRangerPolicy(rangerPolicy);
                } else {
                    rangerPolicy.getPolicyLabels().remove(getPersonaLabel(persona.getGuid()));
                    rangerPolicy.getPolicyLabels().removeAll(removePolicyGuids);

                    long policyLabelCount = rangerPolicy.getPolicyLabels().stream().filter(x -> x.startsWith(LABEL_PREFIX_PERSONA)).count();
                    if (policyLabelCount == 0) {
                        rangerPolicy.getPolicyLabels().remove(LABEL_TYPE_PERSONA);
                    }

                    atlasRangerService.updateRangerPolicy(rangerPolicy);
                }
            } catch (AtlasBaseException e) {
                LOG.error("Failed to clean Ranger role from Ranger policies: {}", e.getMessage());
            } finally {
                LOG.info("End CleanRoleWorker");
            }

            return ret;
        }

        private boolean cleanRoleFromAccessPolicy(String role, RangerPolicy policy) {
            for (RangerPolicyItem policyItem : new ArrayList<>(policy.getPolicyItems())) {
                if (policyItem.getRoles().remove(role)) {
                    if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                        policy.getPolicyItems().remove(policyItem);

                        if (CollectionUtils.isEmpty(policy.getDenyPolicyItems()) && CollectionUtils.isEmpty(policy.getPolicyItems())) {
                            return true;
                        }
                    }
                }
            }

            for (RangerPolicyItem policyItem : new ArrayList<>(policy.getDenyPolicyItems())) {
                if (policyItem.getRoles().remove(role)) {
                    if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                        policy.getDenyPolicyItems().remove(policyItem);

                        if (CollectionUtils.isEmpty(policy.getDenyPolicyItems()) && CollectionUtils.isEmpty(policy.getPolicyItems())) {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        private boolean cleanRoleFromMaskingPolicy(String role, RangerPolicy policy) {
            List<RangerDataMaskPolicyItem> policyItemsToUpdate = policy.getDataMaskPolicyItems();

            for (RangerDataMaskPolicyItem policyItem : new ArrayList<>(policyItemsToUpdate)) {
                if (policyItem.getRoles().remove(role)) {
                    if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                        policyItemsToUpdate.remove(policyItem);

                        if (CollectionUtils.isEmpty(policy.getDataMaskPolicyItems())) {
                            return true;
                        }
                    }
                }
            }

            return false;
        }
    }

    private static class UpdateRangerPolicyWorker implements Callable<RangerPolicy> {
        private static final Logger LOG = LoggerFactory.getLogger(UpdateRangerPolicyWorker.class);

        private PersonaContext context;
        private RangerPolicy rangerPolicy;
        private RangerPolicy provisionalPolicy;

        public UpdateRangerPolicyWorker(PersonaContext context, RangerPolicy rangerPolicy, RangerPolicy provisionalPolicy) {
            this.context = context;
            this.rangerPolicy = rangerPolicy;
            this.provisionalPolicy = provisionalPolicy;
        }

        @Override
        public RangerPolicy call() {
            RangerPolicy ret = null;
            LOG.info("Starting UpdateRangerPolicyWorker");

            try {
                boolean addNewPolicyItem = false;

                if (rangerPolicy == null) {
                    RangerPolicy policy = fetchRangerPolicyByResources(atlasRangerService,
                            context.isDataPolicy() ? RANGER_HEKA_SERVICE_TYPE.getString() : RANGER_ATLAS_SERVICE_TYPE.getString(),
                            context.isDataMaskPolicy() ? RANGER_POLICY_TYPE_DATA_MASK : RANGER_POLICY_TYPE_ACCESS,
                            provisionalPolicy);

                    if (policy == null) {
                        atlasRangerService.createRangerPolicy(provisionalPolicy);
                    } else {
                        rangerPolicy = policy;
                        addNewPolicyItem = true;
                    }
                }

                if (rangerPolicy != null) {
                    boolean update = false;

                    if (context.isDataMaskPolicy()) {
                        List<RangerDataMaskPolicyItem> existingRangerPolicyItems = rangerPolicy.getDataMaskPolicyItems();
                        List<RangerDataMaskPolicyItem> provisionalPolicyItems = provisionalPolicy.getDataMaskPolicyItems();

                        update = updateMaskPolicyItem(context,
                                existingRangerPolicyItems,
                                provisionalPolicyItems,
                                addNewPolicyItem);

                    } else {

                        List<RangerPolicyItem> existingRangerPolicyItems = context.isAllowPolicy() ?
                                rangerPolicy.getPolicyItems() :
                                rangerPolicy.getDenyPolicyItems();

                        List<RangerPolicyItem> provisionalPolicyItems = context.isAllowPolicy() ?
                                provisionalPolicy.getPolicyItems() :
                                provisionalPolicy.getDenyPolicyItems();

                        update = updatePolicyItem(context,
                                existingRangerPolicyItems,
                                provisionalPolicyItems,
                                addNewPolicyItem);
                    }

                    if (update) {
                        List<String> labels = rangerPolicy.getPolicyLabels();
                        labels.add(getPersonaLabel(context.getPersona().getGuid()));
                        labels.add(getPersonaPolicyLabel(context.getPersonaPolicy().getGuid()));
                        rangerPolicy.setPolicyLabels(labels);

                        atlasRangerService.updateRangerPolicy(rangerPolicy);
                    }
                }
            } catch (AtlasBaseException e) {
                LOG.error("Failed to update Ranger policies: {}", e.getMessage());
            } finally {
                LOG.info("End UpdateRangerPolicyWorker");
            }

            return ret;
        }

        private boolean updatePolicyItem(PersonaContext context,
                                         List<RangerPolicyItem> existingRangerPolicyItems,
                                         List<RangerPolicyItem> provisionalPolicyItems,
                                         boolean addNewPolicyItem) {


            if (addNewPolicyItem || CollectionUtils.isEmpty(existingRangerPolicyItems)) {
                //no condition present at all
                //add new condition & update existing Ranger policy
                existingRangerPolicyItems.add(provisionalPolicyItems.get(0));

            } else {
                String role = getQualifiedName(context.getPersona());

                List<RangerPolicyItem> temp = new ArrayList<>(existingRangerPolicyItems);

                for (int i = 0; i < temp.size(); i++) {
                    RangerPolicyItem policyItem = temp.get(i);

                    if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                        List<RangerPolicyItemAccess> newAccesses = provisionalPolicyItems.get(0).getAccesses();

                        if (CollectionUtils.isEqualCollection(policyItem.getAccesses(), newAccesses)) {
                            //accesses are equal, do not update
                            return false;
                        }

                        if (CollectionUtils.isNotEmpty(policyItem.getGroups()) || CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                            //contaminated policyItem,
                            // remove role from policy Item
                            // Add another policy item specific for persona role
                            existingRangerPolicyItems.get(i).getRoles().remove(role);
                            existingRangerPolicyItems.add(provisionalPolicyItems.get(0));
                            continue;
                        }

                        existingRangerPolicyItems.get(i).setAccesses(provisionalPolicyItems.get(0).getAccesses());
                    }
                }
            }

            return true;
        }

        private boolean updateMaskPolicyItem(PersonaContext context,
                                             List<RangerDataMaskPolicyItem> existingRangerPolicyItems,
                                             List<RangerDataMaskPolicyItem> provisionalPolicyItems,
                                             boolean addNewPolicyItem) {

            if (addNewPolicyItem || CollectionUtils.isEmpty(existingRangerPolicyItems)) {
                //no condition present at all
                //add new condition & update existing Ranger policy
                existingRangerPolicyItems.add(provisionalPolicyItems.get(0));

            } else {
                String role = getQualifiedName(context.getPersona());

                List<RangerDataMaskPolicyItem> temp = new ArrayList<>(existingRangerPolicyItems);

                for (int i = 0; i < temp.size(); i++) {
                    RangerDataMaskPolicyItem policyItem = temp.get(i);

                    if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                        List<RangerPolicyItemAccess> newAccesses = provisionalPolicyItems.get(0).getAccesses();
                        RangerPolicyItemDataMaskInfo newMaskInfo = provisionalPolicyItems.get(0).getDataMaskInfo();

                        if (CollectionUtils.isEqualCollection(policyItem.getAccesses(), newAccesses) &&
                                policyItem.getDataMaskInfo().equals(newMaskInfo)) {
                            //accesses & mask info are equal, do not update
                            return false;
                        }

                        if (CollectionUtils.isNotEmpty(policyItem.getGroups()) || CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                            //contaminated policyItem,
                            // remove role from policy Item
                            // Add another policy item specific for persona role
                            existingRangerPolicyItems.get(i).getRoles().remove(role);
                            existingRangerPolicyItems.add(provisionalPolicyItems.get(0));
                            continue;
                        }

                        existingRangerPolicyItems.get(i).setAccesses(provisionalPolicyItems.get(0).getAccesses());
                        existingRangerPolicyItems.get(i).setDataMaskInfo(provisionalPolicyItems.get(0).getDataMaskInfo());
                    }
                }
            }

            return true;
        }
    }
}
