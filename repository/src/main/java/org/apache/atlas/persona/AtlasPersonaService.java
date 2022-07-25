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
package org.apache.atlas.persona;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ESAliasStore;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerRole;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.apache.atlas.ranger.AtlasRangerService;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.persona.AtlasPersonaUtil.*;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_TERM_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_GLOSSARY_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_METADATA_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;


@Component
public class AtlasPersonaService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPersonaService.class);

    private AtlasRangerService atlasRangerService;
    private final AtlasEntityStore entityStore;
    private final AtlasGraph graph;
    private final ESAliasStore aliasStore;
    private final EntityGraphRetriever entityRetriever;
    private final EntityDiscoveryService entityDiscoveryService;

    @Inject
    public AtlasPersonaService(AtlasRangerService atlasRangerService,
                               AtlasEntityStore entityStore,
                               EntityDiscoveryService entityDiscoveryService,
                               EntityGraphRetriever entityRetriever,
                               ESAliasStore aliasStore,
                               AtlasGraph graph) {
        this.entityStore = entityStore;
        this.atlasRangerService = atlasRangerService;
        this.graph = graph;
        this.aliasStore = aliasStore;
        this.entityDiscoveryService = entityDiscoveryService;
        this.entityRetriever = entityRetriever;
    }

    public EntityMutationResponse createOrUpdatePersona(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException, JSONException, IOException {
        EntityMutationResponse ret = null;
        try {
            AtlasEntity persona = entityWithExtInfo.getEntity();
            AtlasEntity existingPersonaEntity = null;

            PersonaContext context = new PersonaContext(entityWithExtInfo);

            try {
                Map<String, Object> uniqueAttributes = mapOf(QUALIFIED_NAME, getQualifiedName(persona));
                existingPersonaEntity = entityRetriever.toAtlasEntity(new AtlasObjectId(persona.getGuid(), persona.getTypeName(), uniqueAttributes));
            } catch (AtlasBaseException abe) {
                if (abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_GUID_NOT_FOUND &&
                        abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                    throw abe;
                }
            }

            if (existingPersonaEntity == null) {
                ret = createPersona(context, entityWithExtInfo);
            } else {
                ret = updatePersona(context, existingPersonaEntity);
            }

        } catch (AtlasBaseException abe) {
            //TODO: handle exception
            LOG.error("Failed to create persona {}", abe.getMessage());
            throw abe;
        }
        return ret;
    }

    private EntityMutationResponse updatePersona(PersonaContext context, AtlasEntity existingPersonaEntity) throws AtlasBaseException, JSONException, IOException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updatePersona");
        LOG.info("Updating Persona");
        EntityMutationResponse ret = null;
        //Atlas Persona entity (existingPersonaEntity) is the source of truth for current state

        AtlasEntity persona = context.getPersona();

        if (getPersonaRoleId(persona) != getPersonaRoleId(existingPersonaEntity)) {
            throw new AtlasBaseException(ATTRIBUTE_UPDATE_NOT_SUPPORTED, PERSONA_ENTITY_TYPE, "rangerRoleId");
        }

        if (getIsEnabled(existingPersonaEntity) != getIsEnabled(context.getPersona())) {
            if (getIsEnabled(context.getPersona())) {
                enablePersona(context);
            } else {
                disablePersona(context);
            }
        }

        //check name update
        // if yes: check naw name for uniqueness
        if (!getName(persona).equals(getName(existingPersonaEntity))) {
            validateUniquenessByName(getName(persona), PERSONA_ENTITY_TYPE);
        }

        RangerRole rangerRole = atlasRangerService.updateRangerRole(context);

        aliasStore.createAlias(context);

        ret = entityStore.createOrUpdate(new AtlasEntityStream(context.getPersona()), false);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    private void enablePersona(PersonaContext context) {
        //TODO:create all policies
        AtlasEntity persona = context.getPersona();

        List<AtlasEntity> personaPolicies = getPersonaAllPolicies(context.getPersonaExtInfo());

        //createPersonaPolicy()
    }

    private void disablePersona(PersonaContext context) throws AtlasBaseException {
        //TODO: clean roles
        AtlasEntity persona = context.getPersona();

        List<AtlasEntity> personaPolicies = getPersonaAllPolicies(context.getPersonaExtInfo());

        List<RangerPolicy> rangerPolicies = getRangerPoliciesByLabel(getPersonaLabel(persona.getGuid()));

        cleanRoleToDisablePersona(context, rangerPolicies, personaPolicies);
    }

    private EntityMutationResponse createPersona(PersonaContext context, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("createPersona");
        LOG.info("Creating Persona");
        EntityMutationResponse ret;
        context.setCreateNewPersona(true);

        //TODO:validateConnectionIdForPersona ????

        validateUniquenessByName(getName(entityWithExtInfo.getEntity()), PERSONA_ENTITY_TYPE);

        //unique qualifiedName for Persona
        String tenantId = getTenantId(context.getPersona());
        if (StringUtils.isEmpty(tenantId)) {
            tenantId = "tenant";
        }
        entityWithExtInfo.getEntity().setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entityWithExtInfo.getEntity().setAttribute("enabled", true);

        RangerRole rangerRole = atlasRangerService.createRangerRole(context);
        context.getPersona().getAttributes().put("rangerRoleId", rangerRole.getId());

        ret = entityStore.createOrUpdate(new AtlasEntityStream(context.getPersona()), false);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }

    /*
    * @Param entityWithExtInfo -> Persona policy entity to be updated
    * */
    public EntityMutationResponse createOrUpdatePersonaPolicy(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException, JSONException, IOException {
        EntityMutationResponse response = null;

        AtlasEntity personaPolicy = entityWithExtInfo.getEntity();
        validatePersonaPolicyRequest(personaPolicy);

        AtlasEntity.AtlasEntityWithExtInfo existingPersonaPolicy = null;
        PersonaContext context = new PersonaContext();

        if (AtlasTypeUtil.isAssignedGuid(personaPolicy.getGuid())) {
            existingPersonaPolicy = entityRetriever.toAtlasEntityWithExtInfo(personaPolicy.getGuid());
            if (existingPersonaPolicy != null) {
                context.setUpdatePersonaPolicy(true);
                context.setExistingPersonaPolicy(existingPersonaPolicy.getEntity());
            } else {
                context.setCreateNewPersonaPolicy(true);
            }
        } else {
            context.setCreateNewPersonaPolicy(true);
        }

        //get Role (Persona) entity
        String personaGuid = getPersonaGuid(personaPolicy);
        AtlasEntity.AtlasEntityWithExtInfo personaWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(personaGuid);

        if (context.isCreateNewPersonaPolicy()) {
            personaPolicy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getQualifiedName(personaWithExtInfo.getEntity()), getUUID()));
        }

        context.setPersonaExtInfo(personaWithExtInfo);
        context.setPersonaPolicy(personaPolicy);

        context.setAllowPolicy(getIsAllow(personaPolicy));
        context.setAllowPolicyUpdate();
        context.setPolicyType();

        //verify Unique name across current Persona's policies
        verifyUniqueNameForPersonaPolicy(context, getName(personaPolicy), personaWithExtInfo);

        //create/update Atlas entity for policy (no commit)
        response = entityStore.createOrUpdateForImportNoCommit(new AtlasEntityStream(personaPolicy));

        if (context.isCreateNewPersonaPolicy()) {
            if (CollectionUtils.isNotEmpty(response.getCreatedEntities())) {
                LOG.info("Create Persona policy no commit response \n{}\n", AtlasType.toJson(response));
                AtlasEntityHeader createdPersonaPolicyHeader = response.getCreatedEntities().get(0);

                personaPolicy.setGuid(createdPersonaPolicyHeader.getGuid());
            } else {
                throw new AtlasBaseException("Failed to create Atlas Entity");
            }

        } else if (CollectionUtils.isEmpty(response.getUpdatedEntities())) {
            throw new AtlasBaseException("Failed to update Atlas Entity");
        }

        switch (personaPolicy.getTypeName()) {
            case PERSONA_METADATA_POLICY_ENTITY_TYPE:
            case PERSONA_GLOSSARY_POLICY_ENTITY_TYPE: createOrUpdatePersonaPolicy(context); break;

        }

        aliasStore.createAlias(context);

        //TODO: enable graph commit to persist persona policy entity
        graph.commit();
        return response;
    }

    private List<RangerPolicy> createOrUpdatePersonaPolicy(PersonaContext context) throws AtlasBaseException {
        List<RangerPolicy> ret = null;

        //convert persona policy into Ranger policies
        List<RangerPolicy> provisionalRangerPolicies = toRangerPolicies(context);

        if (CollectionUtils.isNotEmpty(provisionalRangerPolicies)) {
            LOG.info(AtlasType.toJson(provisionalRangerPolicies));

            if (context.isCreateNewPersonaPolicy()) {
                ret = createPersonaPolicy(context, provisionalRangerPolicies);
            } else {
                ret = updatePersonaPolicy(context, provisionalRangerPolicies);
            }
        } else {
            throw new AtlasBaseException("provisionalRangerPolicies could not be empty");
        }

        return ret;
    }

    private List<RangerPolicy> createPersonaPolicy(PersonaContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> ret = new ArrayList<>();

        //verify that this is unique policy for current Persona
        verifyUniquePersonaPolicy(context, null);

        for (RangerPolicy provisionalPolicy : provisionalRangerPolicies) {
            //check if there is existing Ranger policy of current provisional Ranger policy
            RangerPolicy rangerPolicy = getRangerPolicyByResources(provisionalPolicy);

            if (rangerPolicy == null) {
                ret.add(atlasRangerService.createRangerPolicy(provisionalPolicy));
            } else {
                List<RangerPolicy.RangerPolicyItem> policyItems;

                if (context.isAllowPolicy()) {
                    rangerPolicy.getPolicyItems().add(provisionalPolicy.getPolicyItems().get(0));
                } else {
                    rangerPolicy.getDenyPolicyItems().add(provisionalPolicy.getDenyPolicyItems().get(0));
                }

                List<String> labels = rangerPolicy.getPolicyLabels();
                labels.addAll(getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid()));
                rangerPolicy.setPolicyLabels(labels);

                RangerPolicy pol = atlasRangerService.updateRangerPolicy(rangerPolicy);
                ret.add(pol);

                LOG.info("Updated Ranger Policy with ID {}", pol.getId());
            }
        }

        ret.forEach(x -> LOG.info("Created \n{}\n", AtlasType.toJson(x)));

        return ret;
    }

    private boolean isAssetUpdate(PersonaContext context) {
        boolean ret = false;
        if (context.isMetadataPolicy()) {
            ret = !CollectionUtils.isEqualCollection(
                    getAssets(context.getExistingPersonaPolicy()),
                    getAssets(context.getPersonaPolicy()));

        } else if (context.isGlossaryPolicy()) {
            ret = !CollectionUtils.isEqualCollection(
                    getGlossaryQualifiedNames(context.getExistingPersonaPolicy()),
                    getGlossaryQualifiedNames(context.getPersonaPolicy()));
        }

        return ret;
    }

    private List<RangerPolicy> updatePersonaPolicy(PersonaContext context, List<RangerPolicy> provisionalRangerPolicies) throws AtlasBaseException {
        List<RangerPolicy> ret = null;
        Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap = new HashMap<>();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        //verify that this is unique policy for current Persona
        verifyUniquePersonaPolicy(context, personaPolicy.getGuid());

        List<RangerPolicy> rangerPolicies = getRangerPoliciesByLabel(getPersonaPolicyLabel(personaPolicy.getGuid()));

        if (context.isUpdateIsAllow() || isAssetUpdate(context)) {
            //Assets update OR allow condition update
            //remove role from existing policies & create new Ranger policies
            cleanRoleFromExistingPolicies(context, rangerPolicies);

            for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
                provisionalToRangerPoliciesMap.put(provisionalRangerPolicy, null);
            }

        } else {
            provisionalToRangerPoliciesMap = mapPolicies(context, provisionalRangerPolicies, rangerPolicies);
        }

        if (MapUtils.isNotEmpty(provisionalToRangerPoliciesMap)) {
            ret = processPolicies(context, provisionalToRangerPoliciesMap);
        }

        processActionsRemoval(context, rangerPolicies);

        return ret;
    }

    private void cleanRoleFromExistingPolicies(PersonaContext context, List<RangerPolicy> rangerPolicies) throws AtlasBaseException {
        String role = getQualifiedName(context.getPersona());

        for (RangerPolicy policy: rangerPolicies) {
            boolean deletePol = false;

            List<RangerPolicy.RangerPolicyItem> policyItemsToUpdate;
            List<RangerPolicy.RangerPolicyItem> tempPolicyItems;

            if (context.isUpdateIsAllow()) {
                policyItemsToUpdate = context.isAllowPolicy() ?
                        new ArrayList(policy.getDenyPolicyItems()) :
                        new ArrayList(policy.getPolicyItems());
            } else {
                policyItemsToUpdate = context.isAllowPolicy() ?
                        new ArrayList(policy.getPolicyItems()) :
                        new ArrayList(policy.getDenyPolicyItems());
            }
            tempPolicyItems = new ArrayList<>(policyItemsToUpdate);

            for (int i = 0; i < tempPolicyItems.size(); i++) {
                RangerPolicy.RangerPolicyItem policyItem = tempPolicyItems.get(i);

                if (CollectionUtils.isNotEmpty(policyItem.getRoles())) {

                    if (policyItemsToUpdate.get(i).getRoles().remove(role)) {

                        if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                            policyItemsToUpdate.remove(policyItem);

                            if (CollectionUtils.isEmpty(policyItemsToUpdate)) {
                                deletePol = true;
                            }
                        }
                    }
                }
            }


            if (deletePol) {
                atlasRangerService.deleteRangerPolicy(policy);
            } else {
                policy.getPolicyLabels().removeAll(getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid()));

                atlasRangerService.updateRangerPolicy(policy);
            }
        }
    }

    /*
    * @Param rangerPolicies -> Persona's all Ranger policies found by Persona label
    * @Param personaPolicies -> All Persona policies in Atlas
    * */
    private void cleanRoleToDisablePersona(PersonaContext context, List<RangerPolicy> rangerPolicies,
                                           List<AtlasEntity> personaPolicies) throws AtlasBaseException {
        String role = getQualifiedName(context.getPersona());

        for (RangerPolicy policy: rangerPolicies) {
            boolean deletePol = false;

            List<RangerPolicy.RangerPolicyItem> policyItemsToUpdate;
            List<RangerPolicy.RangerPolicyItem> tempPolicyItems;

            boolean existingAllowFlag = getIsAllow(context.getExistingPersonaPolicy());

            policyItemsToUpdate = existingAllowFlag ?
                        new ArrayList<>(policy.getPolicyItems()) :
                        new ArrayList<>(policy.getDenyPolicyItems());

            tempPolicyItems = new ArrayList<>(policyItemsToUpdate);

            for (int i = 0; i < tempPolicyItems.size(); i++) {
                RangerPolicy.RangerPolicyItem policyItem = tempPolicyItems.get(i);

                if (CollectionUtils.isNotEmpty(policyItem.getRoles())) {

                    if (policyItemsToUpdate.get(i).getRoles().remove(role)) {

                        if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
                            policyItemsToUpdate.remove(policyItem);

                            if (CollectionUtils.isEmpty(policyItemsToUpdate)) {
                                deletePol = true;
                            }
                        }
                    }
                }
            }

            if (deletePol) {
                atlasRangerService.deleteRangerPolicy(policy);
            } else {
                policy.getPolicyLabels().removeAll(getLabelsForPersonaPolicy(context.getPersona().getGuid(), context.getPersonaPolicy().getGuid()));

                atlasRangerService.updateRangerPolicy(policy);
            }
        }
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
    private void processActionsRemoval(PersonaContext context, List<RangerPolicy> existingRangerPolicies) throws AtlasBaseException {
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

                        List<RangerPolicy.RangerPolicyItem> policyItemsToUpdate;

                        if (context.isAllowPolicy()) {
                            policyItemsToUpdate = rangerPolicy.getPolicyItems();
                        } else {
                            policyItemsToUpdate = rangerPolicy.getDenyPolicyItems();
                        }

                        List<RangerPolicy.RangerPolicyItem> tempPoliciesItems = new ArrayList<>(policyItemsToUpdate);

                        for (int j = 0; j < tempPoliciesItems.size(); j++) {
                            RangerPolicy.RangerPolicyItem policyItem = tempPoliciesItems.get(j);
                            boolean update = false;

                            if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                                if (CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getUsers())) {
                                    policyItemsToUpdate.remove(policyItem);

                                    if (CollectionUtils.isEmpty(policyItemsToUpdate)) {
                                        atlasRangerService.deleteRangerPolicy(rangerPolicy);
                                        //break;
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
                        //TODO: find Ranger policy by resource search
                        //Rare Case, if label removed from Ranger policy manually
                    }
                }
            }
        }
    }

    private List<RangerPolicy> processPolicies(PersonaContext context,
                                 Map<RangerPolicy, RangerPolicy> provisionalToRangerPoliciesMap) throws AtlasBaseException {
        if (MapUtils.isEmpty(provisionalToRangerPoliciesMap)) {
            throw new AtlasBaseException("Policies map is empty");
        }

        List<RangerPolicy> ret = new ArrayList<>();

        for (Map.Entry<RangerPolicy, RangerPolicy> policyPair : provisionalToRangerPoliciesMap.entrySet()) {
            RangerPolicy provisionalPolicy = policyPair.getKey();
            RangerPolicy existingRangerPolicy = policyPair.getValue();

            boolean isCreate = false;

            if (existingRangerPolicy == null) {
                //no matching policy found with label
                //now search policy by resources
                    //if found, update
                    //if not found, create new

                RangerPolicy rangerPolicy = getRangerPolicyByResources(provisionalPolicy);

                /*if (CollectionUtils.isNotEmpty(rangerPolicies)) {
                    //find exact match among the result list
                    String provisionalPolicyResourcesSignature = new RangerPolicyResourceSignature(provisionalPolicy).getSignature();

                    for (RangerPolicy resourceMatchedPolicy : rangerPolicies) {
                        String resourceMatchedPolicyResourcesSignature = new RangerPolicyResourceSignature(resourceMatchedPolicy).getSignature();

                        if (provisionalPolicyResourcesSignature.equals(resourceMatchedPolicyResourcesSignature)) {
                            existingRangerPolicy = resourceMatchedPolicy;
                            break;
                        }
                    }
                }*/

                if (rangerPolicy != null) {
                    existingRangerPolicy = rangerPolicy;
                }

                if (existingRangerPolicy == null) {
                    RangerPolicy pol = atlasRangerService.createRangerPolicy(provisionalPolicy);
                    ret.add(pol);
                    isCreate = true;

                    LOG.info("Created Ranger Policy with ID {}", pol.getId());
                }
            }

            if (!isCreate) {
                //policy mapping found, means matching Ranger policy is present
                //check if policy item with exact single role is present
                boolean skipUpdate = false;


                if (context.isAllowPolicy() && CollectionUtils.isEmpty(existingRangerPolicy.getPolicyItems())) {
                    //no condition present at all
                    //add new condition & update existing Ranger policy
                    existingRangerPolicy.getPolicyItems().add(provisionalPolicy.getPolicyItems().get(0));

                } else if (!context.isAllowPolicy() && CollectionUtils.isEmpty(existingRangerPolicy.getDenyPolicyItems())) {

                    existingRangerPolicy.getDenyPolicyItems().add(provisionalPolicy.getDenyPolicyItems().get(0));

                } else {
                    String role = getQualifiedName(context.getPersona());

                    List<RangerPolicy.RangerPolicyItem> temp = context.isAllowPolicy() ?
                            new ArrayList<>(existingRangerPolicy.getPolicyItems()) :
                            new ArrayList<>(existingRangerPolicy.getDenyPolicyItems());


                    for (int i = 0; i < temp.size(); i++) {
                        RangerPolicy.RangerPolicyItem policyItem = temp.get(i);

                        if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && policyItem.getRoles().contains(role)) {

                            List<RangerPolicy.RangerPolicyItemAccess> newAccesses = context.isAllowPolicy() ?
                                    provisionalPolicy.getPolicyItems().get(0).getAccesses() :
                                    provisionalPolicy.getDenyPolicyItems().get(0).getAccesses();

                            if (CollectionUtils.isEqualCollection(policyItem.getAccesses(), newAccesses)) {
                                skipUpdate = true;
                                continue;
                            }

                            if (CollectionUtils.isNotEmpty(policyItem.getGroups()) || CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                                //contaminated policyItem,
                                // remove role from policy Item
                                // Add another policy item specific for persona role
                                if (context.isAllowPolicy()) {
                                    existingRangerPolicy.getPolicyItems().get(i).getRoles().remove(role);
                                    existingRangerPolicy.getPolicyItems().add(provisionalPolicy.getPolicyItems().get(0));
                                } else {
                                    existingRangerPolicy.getDenyPolicyItems().get(i).getRoles().remove(role);
                                    existingRangerPolicy.getDenyPolicyItems().add(provisionalPolicy.getDenyPolicyItems().get(0));
                                }

                                continue;
                            }


                            if (context.isAllowPolicy()) {
                                existingRangerPolicy.getPolicyItems().get(i).setAccesses(provisionalPolicy.getPolicyItems().get(0).getAccesses());
                            } else {
                                existingRangerPolicy.getDenyPolicyItems().get(i).setAccesses(provisionalPolicy.getDenyPolicyItems().get(0).getAccesses());
                            }
                        }
                    }

                }

                if (!skipUpdate) {
                    List<String> labels = existingRangerPolicy.getPolicyLabels();
                    labels.add(getPersonaLabel(context.getPersona().getGuid()));
                    labels.add(getPersonaPolicyLabel(context.getPersonaPolicy().getGuid()));
                    existingRangerPolicy.setPolicyLabels(labels);

                    RangerPolicy pol = atlasRangerService.updateRangerPolicy(existingRangerPolicy);
                    ret.add(pol);

                    LOG.info("Updated Ranger Policy with ID {}", pol.getId());
                }
            }
        }
        return ret;
    }

    /*
    * @Param provisionalRangerPolicies -> Policies transformed from AtlasPersonaPolicy to Ranger policy
    * @Param existingRangerPolicies -> Policies found by label search
    * */
    private Map<RangerPolicy, RangerPolicy> mapPolicies(PersonaContext context,
                                                        List<RangerPolicy> provisionalRangerPolicies,
                                                        List<RangerPolicy> existingRangerPolicies) {

        Map<RangerPolicy, RangerPolicy> ret = new HashMap<>();

        /*for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
            //find existing Ranger policy corresponding to provisional Ranger policy
            ret.put(provisionalRangerPolicy, null);
            String provisionalRangerPolicySignature = new RangerPolicyResourceSignature(provisionalRangerPolicy).getSignature();


            for (RangerPolicy existingRangerPolicy: existingRangerPolicies) {
                String existingRangerPolicySignature = new RangerPolicyResourceSignature(existingRangerPolicy).getSignature();

                if (existingRangerPolicySignature.equals(provisionalRangerPolicySignature)) {
                    ret.put(provisionalRangerPolicy, existingRangerPolicy);
                    break;
                }
            }
        }*/

        for (RangerPolicy existingRangerPolicy: existingRangerPolicies) {
            boolean mapped = false;
            String existingRangerPolicySignature = new RangerPolicyResourceSignature(existingRangerPolicy).getSignature();

            for (RangerPolicy provisionalRangerPolicy: provisionalRangerPolicies) {
                String provisionalRangerPolicySignature = new RangerPolicyResourceSignature(provisionalRangerPolicy).getSignature();

                if (existingRangerPolicySignature.equals(provisionalRangerPolicySignature)) {
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

    private List<RangerPolicy> getRangerPoliciesByLabel(String label) throws AtlasBaseException {
        List<RangerPolicy> ret = new ArrayList<>();

        Map <String, String> params = new HashMap<>();
        int size = 25;
        int from = 0;

        params.put("policyLabelsPartial", label);
        params.put("policyType", "0"); //POLICY_TYPE_ACCESS
        params.put("page", "0");
        params.put("pageSize", String.valueOf(size));
        params.put("serviceType", AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE.getString());

        int fetched;
        do {
            params.put("startIndex", String.valueOf(from));

            List<RangerPolicy> rangerPolicies = atlasRangerService.getPoliciesByLabel(params);
            fetched = rangerPolicies.size();
            ret.addAll(rangerPolicies);

            from += size;

        } while (fetched == size);

        return ret;
    }

    private RangerPolicy getRangerPolicyByResources(RangerPolicy policy) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        Map<String, String> resourceForSearch = new HashMap<>();
        for (String resourceName : policy.getResources().keySet()) {

            RangerPolicy.RangerPolicyResource value = policy.getResources().get(resourceName);
            resourceForSearch.put(resourceName, value.getValues().get(0));
        }

        LOG.info("resourceForSearch {}", AtlasType.toJson(resourceForSearch));

        Map <String, String> params = new HashMap<>();
        int size = 25;
        int from = 0;

        params.put("policyType", "0"); //POLICY_TYPE_ACCESS
        params.put("page", "0");
        params.put("pageSize", String.valueOf(size));
        params.put("serviceType", AtlasConfiguration.RANGER_ATLAS_SERVICE_TYPE.getString());

        int fetched;
        do {
            params.put("startIndex", String.valueOf(from));

            List<RangerPolicy> rangerPoliciesPaginated = atlasRangerService.getPoliciesByResources(resourceForSearch, params);
            fetched = rangerPoliciesPaginated.size();
            rangerPolicies.addAll(rangerPoliciesPaginated);

            from += size;

        } while (fetched == size);

        if (CollectionUtils.isNotEmpty(rangerPolicies)) {
            //find exact match among the result list
            String provisionalPolicyResourcesSignature = new RangerPolicyResourceSignature(policy).getSignature();

            for (RangerPolicy resourceMatchedPolicy : rangerPolicies) {
                String resourceMatchedPolicyResourcesSignature = new RangerPolicyResourceSignature(resourceMatchedPolicy).getSignature();

                if (provisionalPolicyResourcesSignature.equals(resourceMatchedPolicyResourcesSignature)) {
                    return resourceMatchedPolicy;
                }
            }
        }

        return null;
    }

    private void verifyUniquePersonaPolicy (PersonaContext context, String guidToExclude) throws AtlasBaseException {
        if (context.isMetadataPolicy()) {
            verifyUniqueMetadataPolicy(context, guidToExclude);
        } else if (context.isGlossaryPolicy()) {
            verifyUniqueGlossaryPolicy(context, guidToExclude);
        }
    }

    private void verifyUniqueMetadataPolicy(PersonaContext context, String guidToExclude) throws AtlasBaseException {

        AtlasEntity newPersonaPolicy = context.getPersonaPolicy();
        List<String> newPersonaPolicyAssets = getAssets(newPersonaPolicy);

        List<AtlasEntity> metadataPolicies = getMetadataPolicies(context.getPersonaExtInfo());

        if (CollectionUtils.isNotEmpty(metadataPolicies)) {
            for (AtlasEntity metadataPolicy : metadataPolicies) {
                List<String> assets = getAssets(metadataPolicy);

                if (!StringUtils.equals(guidToExclude, metadataPolicy.getGuid()) && assets.equals(newPersonaPolicyAssets)) {
                    //TODO: Ranger error code
                    throw new AtlasBaseException("Not allowed to create duplicate policy for same assets, existing policy name " + getName(metadataPolicy));
                }
            }
        }
    }

    private void verifyUniqueGlossaryPolicy(PersonaContext context, String guidToExclude) throws AtlasBaseException {

        AtlasEntity newPersonaPolicy = context.getPersonaPolicy();
        List<String> newPersonaPolicyAssets = getGlossaryQualifiedNames(newPersonaPolicy);

        List<AtlasEntity> policies = getGlossaryPolicies(context.getPersonaExtInfo());

        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity policy : policies) {
                List<String> assets = getGlossaryQualifiedNames(policy);

                if (!StringUtils.equals(guidToExclude, policy.getGuid()) && assets.equals(newPersonaPolicyAssets)) {
                    //TODO: Ranger error code
                    throw new AtlasBaseException("Not allowed to create duplicate policy for same glossaries, existing policy name " + getName(policy));
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
    private List<RangerPolicy> toRangerPolicies(PersonaContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("toRangerPolicies");
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity personaPolicy = context.getPersonaPolicy();


        rangerPolicies = personaPolicyToRangerPolicies(context, getActions(context.getPersonaPolicy()));

        //TODO: data policies


        RequestContext.get().endMetricRecord(metricRecorder);

        return rangerPolicies;
    }

    private List<RangerPolicy> personaPolicyToRangerPolicies(PersonaContext context, List<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();

        if (context.isMetadataPolicy()) {
            rangerPolicies = metadataPolicyToRangerPolicy(context, actions);
        } else if (context.isGlossaryPolicy()) {
            rangerPolicies = glossaryPolicyToRangerPolicy(context, actions);
            LOG.info("glossaryPolicyToRangerPolicy : /n{}/n", AtlasType.toJson(rangerPolicies));
        }

        return rangerPolicies;
    }

    private List<RangerPolicy> glossaryPolicyToRangerPolicy(PersonaContext context, List<String> actions) throws AtlasBaseException {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        AtlasEntity persona = context.getPersona();
        AtlasEntity personaPolicy = context.getPersonaPolicy();

        List<String> assets = getGlossaryQualifiedNames(personaPolicy);
        if (CollectionUtils.isEmpty(assets)) {
            throw new AtlasBaseException("Glossary qualified name list is empty");
        }

        List<String> rangerPolicyItemAssets = new ArrayList<>();
        assets.forEach(x -> rangerPolicyItemAssets.add("*" + x + "*"));

        for (String action : new HashSet<>(actions)) {

            if (action.equals("entity-create")) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);
                String name = "Glossary-term-relationship-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));


                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
            }

            if (ENTITY_ACTIONS.contains(action) && !context.hasEntityActions()) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);

                String name = "Glossary-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));

                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
                context.setHasEntityActions(true);
            }

            if (LABEL_ACTIONS.contains(action) && !context.hasEntityLabelActions()) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);
                String name = "Glossary-labels-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));

                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_ENTITY_LABEL, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : LABEL_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
                context.setHasEntityLabelActions(true);
            }

            if (CLASSIFICATION_ACTIONS.contains(action) && !context.hasEntityClassificationActions()) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);
                String name = "Glossary-classification-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
                context.setHasEntityClassificationActions(true);
            }

            if (BM_ACTION.equals(action) && !context.hasEntityBMActions()) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);
                String name = "Glossary-entity-business-metadata-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();


                resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(GLOSSARY_TYPES, false, false));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_BM, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(new RangerPolicy.RangerPolicyItemAccess(BM_ACTION));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.add(rangerPolicy);
                context.setHasEntityBMActions(true);
            }

            if (LINK_ASSET_ACTION.equals(action) && !context.hasLinkAssetsActions()) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);

                String name = "Glossary-relationship-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                rangerPolicies.addAll(glossaryPolicyToRangerPolicy(context, Arrays.asList("related-terms")));

                context.setHasLinkAssetsActions(true);
                rangerPolicies.add(rangerPolicy);
            }

            if (action.equals("related-terms")) {
                RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);

                String name = "Glossary-related-terms-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_UPDATE_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

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

    private List<RangerPolicy> metadataPolicyToRangerPolicy(PersonaContext context, List<String> actions) throws AtlasBaseException {
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
            RangerPolicy rangerPolicy = getRangerPolicy(persona, personaPolicy);

            if (ENTITY_ACTIONS.contains(action) && !context.hasEntityActions()) {

                String name = "CRUD-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                //TODO : Persona.go: 1328
                if (getName(persona).startsWith("collection") || getName(persona).startsWith("connection")) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : ENTITY_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                context.setHasEntityActions(true);
            }

            if (CLASSIFICATION_ACTIONS.contains(action) && !context.hasEntityClassificationActions()) {
                String name = "classification-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                //TODO : Persona.go: 1328
                if (getName(persona).startsWith("collection") || getName(persona).startsWith("connection")) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : CLASSIFICATION_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess(entityAction));
                    }
                }
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                context.setHasEntityClassificationActions(true);
            }

            if ("entity-update-business-metadata".equals(action) && !context.hasEntityBMActions()) {
                String name = "entity-business-metadata-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                //TODO : Persona.go: 1328
                if (getName(persona).startsWith("collection") || getName(persona).startsWith("connection")) {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                } else {
                    resources.put(RESOURCE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Process", "Catalog"), false, false));
                }

                resources.put(RESOURCE_BM, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_KEY_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(new RangerPolicy.RangerPolicyItemAccess("entity-update-business-metadata"));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                context.setHasEntityBMActions(true);
            }

            if (TERM_ACTIONS.contains(action) && !context.hasTermActions()) {
                String name = "terms-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();

                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(ATLAS_GLOSSARY_TERM_ENTITY_TYPE));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));


                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
                for (String entityAction : TERM_ACTIONS) {
                    if (actions.contains(entityAction)) {
                        accesses.add(new RangerPolicy.RangerPolicyItemAccess("add-terms".equals(entityAction) ? ACCESS_ADD_REL : ACCESS_REMOVE_REL));
                    }
                }
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                context.setHasTermActions(true);
            }

            if ("link-assets".equals(action) && !context.hasLinkAssetsActions()) {
                String name = "link-assets-" + UUID.randomUUID();
                rangerPolicy.setName(name);

                Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
                resources.put(RESOURCE_REL_TYPE, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_ONE_ENTITY, new RangerPolicy.RangerPolicyResource(rangerPolicyItemAssets, false, false));
                resources.put(RESOURCE_END_ONE_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_ONE_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                resources.put(RESOURCE_END_TWO_ENTITY, new RangerPolicy.RangerPolicyResource("*"));
                resources.put(RESOURCE_END_TWO_ENTITY_TYPE, new RangerPolicy.RangerPolicyResource(Arrays.asList("Catalog", "Connection", "Dataset", "Infrastructure", "Process", "ProcessExecution", "Namespace"), false, false));
                resources.put(RESOURCE_END_TWO_ENTITY_CLASS, new RangerPolicy.RangerPolicyResource("*"));

                rangerPolicy.setResources(resources);

                List<RangerPolicy.RangerPolicyItemAccess> accesses = Arrays.asList(
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_ADD_REL),
                        new RangerPolicy.RangerPolicyItemAccess(ACCESS_REMOVE_REL));

                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem(accesses, null,
                        null, Arrays.asList(getRoleName(persona)), null, false);

                if (context.isAllowPolicy()) {
                    rangerPolicy.setPolicyItems(Arrays.asList(policyItem));
                } else {
                    rangerPolicy.setDenyPolicyItems(Arrays.asList(policyItem));
                }

                context.setHasLinkAssetsActions(true);
            }

            if (MapUtils.isNotEmpty(rangerPolicy.getResources())) {
                rangerPolicies.add(rangerPolicy);
            }
        }

        return rangerPolicies;
    }

    private RangerPolicy getRangerPolicy(AtlasEntity persona, AtlasEntity personaPolicy){
        RangerPolicy rangerPolicy = new RangerPolicy();

        rangerPolicy.setPolicyLabels(Arrays.asList(
                "type:persona", //TODO: just for testing purpose
                getPersonaLabel(persona.getGuid()),
                getPersonaPolicyLabel(personaPolicy.getGuid())));

        rangerPolicy.setPolicyType(0); //access type policy

        rangerPolicy.setService("atlas");

        return rangerPolicy;
    }

    private void validatePersonaPolicyRequest(AtlasEntity personaPolicy) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(getActions(personaPolicy))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide actions for persona policy");
        }

        if (PERSONA_METADATA_POLICY_ENTITY_TYPE.equals(personaPolicy.getTypeName())) {
            if (CollectionUtils.isEmpty(getAssets(personaPolicy))) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide assets for persona policy");
            }
        }
    }

    private void validateUniquenessByName(String name, String typeName) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("size", 1);

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", typeName)));
        mustClauseList.add(mapOf("term", mapOf("name.keyword", name)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);

        AtlasSearchResult atlasSearchResult = entityDiscoveryService.directIndexSearch(indexSearchParams);

        if (CollectionUtils.isNotEmpty(atlasSearchResult.getEntities())){
            throw new AtlasBaseException(String.format("Entity already exists, typeName:name, %s:%s", typeName, name));
        }
    }

    private void verifyUniqueNameForPersonaPolicy(PersonaContext context, String policyName,
                                                  AtlasEntity.AtlasEntityWithExtInfo personaWithExtInfo) throws AtlasBaseException {

        if (context.isUpdatePersonaPolicy() && !getName(context.getExistingPersonaPolicy()).equals(getName(context.getPersona()))) {
            return;
        }
        List<String> policyNames = new ArrayList<>();

        List<AtlasEntity> personaPolicies = getPersonaAllPolicies(personaWithExtInfo);
        if (CollectionUtils.isNotEmpty(personaPolicies)) {
            personaPolicies.forEach(x -> policyNames.add(getName(x)));
        }

        if (policyNames.contains(policyName)) {
            //TODO: Ranger error code
            throw new AtlasBaseException("Persona policy already exists with same name");
        }
    }
}
