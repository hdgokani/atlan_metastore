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


import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.aliasstore.IndexAliasStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.EntityStream;
import org.apache.atlas.repository.store.users.KeycloakStore;
import org.apache.atlas.transformer.ConnectionPoliciesTransformer;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.keycloak.representations.idm.RoleRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;

public class ConnectionPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPreProcessor.class);

    private static final String CONN_NAME_PATTERN = "connection_admins_%s";

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;
    private AtlasEntityStore entityStore;
    private ConnectionPoliciesTransformer transformer;
    private KeycloakStore keycloakStore;

    public ConnectionPreProcessor(AtlasGraph graph,
                                  AtlasTypeRegistry typeRegistry,
                                  EntityGraphRetriever entityRetriever,
                                  AtlasEntityStore entityStore) {
        this.graph = graph;
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityRetriever;
        this.entityStore = entityStore;

        aliasStore = new ESAliasStore(graph, entityRetriever);
        transformer = new ConnectionPoliciesTransformer();
        keycloakStore = new KeycloakStore();
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
                processCreateConnection(entity);
                break;
            case UPDATE:
                processUpdateConnection(context, entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreateConnection(AtlasStruct struct) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateConnection");

        AtlasEntity connection = (AtlasEntity) struct;

        //create connection role
        String roleName = String.format(CONN_NAME_PATTERN, connection.getGuid());

        List<String> adminUsers = (List<String>) connection.getAttribute("adminUsers");
        List<String> adminGroups = (List<String>) connection.getAttribute("adminGroups");
        List<String> adminRoles = (List<String>) connection.getAttribute("adminRoles");

        RoleRepresentation role = keycloakStore.createRoleForConnection(roleName, true, adminUsers, adminGroups, adminRoles);

        //create connection bootstrap policies
        AtlasEntitiesWithExtInfo policies = transformer.transform(connection, role.getName());
        try {
            RequestContext.get().setPoliciesBootstrappingInProgress(true);
            EntityStream entityStream = new AtlasEntityStream(policies);
            entityStore.createOrUpdate(entityStream, false);
        } finally {
            RequestContext.get().setPoliciesBootstrappingInProgress(false);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateConnection(EntityMutationContext context,
                                      AtlasStruct entity,
                                      AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateConnection");

        //TODO

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    public void processDelete(AtlasVertex vertex) throws AtlasBaseException {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = entityRetriever.toAtlasEntityWithExtInfo(vertex);
        AtlasEntity purpose = entityWithExtInfo.getEntity();

        //TODO
        //delete connection policies

        //delete connection role

    }

    public static String createQualifiedName() {
        return getUUID();
    }
}
