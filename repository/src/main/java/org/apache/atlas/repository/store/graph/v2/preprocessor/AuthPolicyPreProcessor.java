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
package org.apache.atlas.repository.store.graph.v2.preprocessor;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.INSTANCE_GUID_NOT_FOUND;

import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_GROUPS;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ROLES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_SERVICE_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_USERS;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityQualifiedName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPolicyServiceName;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;

public class AuthPolicyPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AuthPolicyPreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;

    public AuthPolicyPreProcessor(AtlasGraph graph,
                                  AtlasTypeRegistry typeRegistry,
                                  EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;

        aliasStore = new ESAliasStore(graph, entityRetriever);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("AuthPolicyPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePolicy(entity);
                break;
            case UPDATE:
                processUpdatePolicy(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePolicy(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePolicy");
        AtlasEntity policy = (AtlasEntity) entity;

        //TODO: validations
        if (StringUtils.isEmpty(getPolicyServiceName(policy))) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide attribute " + ATTR_POLICY_SERVICE_NAME);
        }

        AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
        if (parent != null) {
            AtlasEntity parentEntity = parent.getEntity();

            policy.setAttribute(QUALIFIED_NAME, String.format("%s/%s", getEntityQualifiedName(parentEntity), getUUID()));
            entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

            if (PERSONA_ENTITY_TYPE.equals(parent.getEntity().getTypeName())) {
                //extract role
                String roleName = getPersonaRoleName(parentEntity);
                List<String> roles = Arrays.asList(roleName);
                policy.setAttribute(ATTR_POLICY_ROLES, roles);

                policy.setAttribute(ATTR_POLICY_USERS, new ArrayList<>());
                policy.setAttribute(ATTR_POLICY_GROUPS, new ArrayList<>());

            } else if (PURPOSE_ENTITY_TYPE.equals(parent.getEntity().getTypeName())) {
                //extract tags
                List<String> purposeTags = getPurposeTags(parentEntity);

                List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

                policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);
            }

            //create ES alias
            aliasStore.updateAlias(parent, policy);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePolicy(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePolicy");

        AtlasEntityWithExtInfo parent = getAccessControlEntity((AtlasEntity) entity);

        if (parent != null) {
            AtlasEntity parentEntity = parent.getEntity();
            AtlasEntity policy = (AtlasEntity) entity;
            AtlasEntity existingPolicy = entityRetriever.toAtlasEntityWithExtInfo(vertex).getEntity();

            String qName = getEntityQualifiedName(existingPolicy);
            policy.setAttribute(QUALIFIED_NAME, qName);

            if (PERSONA_ENTITY_TYPE.equals(parent.getEntity().getTypeName())) {
                //extract role
                String roleName = getPersonaRoleName(parentEntity);
                List<String> roles = Arrays.asList(roleName);

                policy.setAttribute(ATTR_POLICY_ROLES, roles);

                policy.setAttribute(ATTR_POLICY_USERS, new ArrayList<>());
                policy.setAttribute(ATTR_POLICY_GROUPS, new ArrayList<>());

            } else if (PURPOSE_ENTITY_TYPE.equals(parent.getEntity().getTypeName())) {
                //extract tags
                List<String> purposeTags = getPurposeTags(parentEntity);

                List<String> policyResources = purposeTags.stream().map(x -> "tag:" + x).collect(Collectors.toList());

                policy.setAttribute(ATTR_POLICY_RESOURCES, policyResources);
            }

            //create ES alias
            parent.addReferredEntity(policy);
            aliasStore.updateAlias(parent, null);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processDeletePolicy");

        try {
            AtlasEntity policy = entityRetriever.toAtlasEntity(vertex);

            if(!policy.getStatus().equals(AtlasEntity.Status.ACTIVE)) {
                LOG.info("Policy with guid {} is already deleted/purged", policy.getGuid());
                return;
            }

            AtlasEntityWithExtInfo parent = getAccessControlEntity(policy);
            if (parent != null) {
                parent.getReferredEntity(policy.getGuid()).setStatus(AtlasEntity.Status.DELETED);
                aliasStore.updateAlias(parent, null);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private AtlasEntityWithExtInfo getAccessControlEntity(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("AuthPolicyPreProcessor.getAccessControl");
        AtlasEntity.AtlasEntityWithExtInfo ret = null;

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        if (objectId != null) {
            try {
                ret = entityRetriever.toAtlasEntityWithExtInfo(objectId);
            } catch (AtlasBaseException abe) {
                AtlasErrorCode code = abe.getAtlasErrorCode();

                if (INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND != code && INSTANCE_GUID_NOT_FOUND != code) {
                    throw abe;
                }
            }
        }

        if (ret != null) {
            List<AtlasObjectId> policies = (List<AtlasObjectId>) ret.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);

            for (AtlasObjectId policy : policies) {
                ret.addReferredEntity(entityRetriever.toAtlasEntity(policy));
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }
}
