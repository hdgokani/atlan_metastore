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
package org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol;


import atlas.keycloak.client.KeycloakClient;
import org.apache.atlas.RequestContext;
import org.apache.atlas.aliasstore.ESAliasStore;
import org.apache.atlas.aliasstore.IndexAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.RoleByIdResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.PURPOSE_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_ROLE_ID;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.getEntityName;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaGroups;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleId;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaUsers;
import static org.apache.atlas.repository.util.AccessControlUtils.getPurposeTags;
import static org.apache.atlas.repository.util.AccessControlUtils.getTenantId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;
import static org.apache.atlas.repository.util.AccessControlUtils.validateUniquenessByName;
import static org.apache.atlas.repository.util.AccessControlUtils.validateUniquenessByTags;

public class PurposePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PurposePreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;

    public PurposePreProcessor(AtlasGraph graph,
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
            LOG.debug("PurposePreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePurpose(entity);
                break;
            case UPDATE:
                processUpdatePurpose(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePurpose(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePurpose");

        String tenantId = getTenantId(entity);

        entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

        //create ES alias
        aliasStore.createAlias((AtlasEntity) entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdatePurpose(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePurpose");

        AtlasEntity purpose = (AtlasEntity) entity;
        AtlasEntity.AtlasEntityWithExtInfo existingPurposeExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity existingPurposeEntity = existingPurposeExtInfo.getEntity();

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        purpose.setAttribute(QUALIFIED_NAME, vertexQName);

        if (!AtlasEntity.Status.ACTIVE.equals(existingPurposeEntity.getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Purpose not Active");
        }

        boolean isEnabled = getIsEnabled(purpose);
        if (getIsEnabled(existingPurposeEntity) != isEnabled) {
            if (isEnabled) {
                //TODO
                //enablePurpose(existingPurposeWithExtInfo);
            } else {
                //TODO
                //disablePurpose(existingPurposeWithExtInfo);
            }
        }

        String newName = getEntityName(purpose);

        if (!newName.equals(getEntityName(existingPurposeEntity))) {
            validateUniquenessByName(graph, newName, PURPOSE_ENTITY_TYPE);
        }

        List<String> newTags = getPurposeTags(purpose);
        if (!CollectionUtils.isEmpty(newTags) && !CollectionUtils.isEqualCollection(newTags, getPurposeTags(existingPurposeEntity))) {
            validateUniquenessByTags(graph, newTags, PURPOSE_ENTITY_TYPE);

            // TODO: update all policies tags resource
            
            aliasStore.updateAlias(existingPurposeExtInfo);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName() {
        return getUUID();
    }
}
