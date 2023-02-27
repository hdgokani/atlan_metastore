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
import joptsimple.internal.Strings;
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
import org.keycloak.admin.client.resource.GroupResource;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_ACCESS_CONTROL_ENABLED;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_PERSONA_ROLE_ID;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_ACCESS_CONTROL;
import static org.apache.atlas.repository.util.AccessControlUtils.getIsEnabled;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaGroups;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaRoleId;
import static org.apache.atlas.repository.util.AccessControlUtils.getPersonaUsers;
import static org.apache.atlas.repository.util.AccessControlUtils.getTenantId;
import static org.apache.atlas.repository.util.AccessControlUtils.getUUID;
import static org.apache.atlas.repository.util.AccessControlUtils.mapOf;

public class PersonaPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PersonaPreProcessor.class);

    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private IndexAliasStore aliasStore;

    public PersonaPreProcessor(AtlasGraph graph,
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
            LOG.debug("PersonaPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreatePersona(entity);
                break;
            case UPDATE:
                processUpdatePersona(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreatePersona(AtlasStruct entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreatePersona");

        String tenantId = getTenantId(entity);

        entity.setAttribute(QUALIFIED_NAME, String.format("%s/%s", tenantId, getUUID()));
        entity.setAttribute(ATTR_ACCESS_CONTROL_ENABLED, true);

        //create keycloak role
        createKeycloakRole((AtlasEntity) entity);

        //create ES alias
        aliasStore.createAlias((AtlasEntity) entity);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void createKeycloakRole(AtlasEntity entity) {
        String roleName = (String) entity.getAttribute(NAME);
        List<String> users = getPersonaUsers(entity);
        List<String> groups = getPersonaGroups(entity);

        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("name", Collections.singletonList(roleName));
        attributes.put("type", Collections.singletonList("persona"));
        attributes.put("level", Collections.singletonList("workspace"));
        attributes.put("createdAt", Collections.singletonList(String.valueOf(System.currentTimeMillis())));
        attributes.put("createdBy", Collections.singletonList(RequestContext.get().getUser()));
        attributes.put("enabled", Collections.singletonList(String.valueOf(true)));
        attributes.put("users", users);
        attributes.put("groups", groups);

        RoleRepresentation roleRepresentation = new RoleRepresentation();
        roleRepresentation.setName(roleName);
        roleRepresentation.setComposite(false);
        roleRepresentation.setAttributes(attributes);

        KeycloakClient.getKeycloakClient().getRealm().roles().create(roleRepresentation);
        LOG.info("Created keycloak role with name {}", roleName);

        String roleId = KeycloakClient.getKeycloakClient().getRealm().roles().get(roleName).toRepresentation().getId();
        roleRepresentation.setId(roleId);

        for (String userName : users) {
            List<UserRepresentation> keyUsers = KeycloakClient.getKeycloakClient().getRealm().users().search(userName);
            Optional<UserRepresentation> keyUserOptional = keyUsers.stream().filter(x -> userName.equals(x.getUsername())).findFirst();
            UserRepresentation keyUser = null;
            if (keyUserOptional.isPresent()) {
                keyUser = keyUserOptional.get();

                final UserResource userResource = KeycloakClient.getKeycloakClient().getRealm().users().get(keyUser.getId());

                userResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));

                userResource.update(keyUser);
            } else {
                try {
                    throw new AtlasBaseException("Keycloak user not found with userName " + userName);
                } catch (AtlasBaseException e) {
                    //TODO: delete keycloak role
                }
            }
        }

        for (String groupName : groups) {
            List<GroupRepresentation> keyGroups = KeycloakClient.getKeycloakClient().getRealm().groups().groups(groupName, 0, 1);
            Optional<GroupRepresentation> keyGroupOptional = keyGroups.stream().filter(x -> groupName.equals(x.getName())).findFirst();
            GroupRepresentation keyGroup = null;
            if (keyGroupOptional.isPresent()) {
                keyGroup = keyGroupOptional.get();

                final GroupResource groupResource = KeycloakClient.getKeycloakClient().getRealm().groups().group(keyGroup.getId());
                groupResource.roles().realmLevel().add(Collections.singletonList(roleRepresentation));

                groupResource.update(keyGroup);
            } else {
                try {
                    throw new AtlasBaseException("Keycloak user not found with userName " + groupName);
                } catch (AtlasBaseException e) {
                    //TODO: delete keycloak role
                }
            }
        }
    }

    private void processUpdatePersona(AtlasStruct entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdatePersona");

        AtlasEntity persona = (AtlasEntity) entity;

        String vertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);
        entity.setAttribute(QUALIFIED_NAME, vertexQName);

        AtlasEntity.AtlasEntityWithExtInfo existingPersonaEntity = entityRetriever.toAtlasEntityWithExtInfo(vertex);

        if (!AtlasEntity.Status.ACTIVE.equals(existingPersonaEntity.getEntity().getStatus())) {
            throw new AtlasBaseException(OPERATION_NOT_SUPPORTED, "Persona not Active");
        }

        if (getPersonaRoleId(persona) != getPersonaRoleId(existingPersonaEntity.getEntity())) {
            throw new AtlasBaseException(ATTRIBUTE_UPDATE_NOT_SUPPORTED, PERSONA_ENTITY_TYPE, ATTR_PERSONA_ROLE_ID);
        }

        boolean isEnabled = getIsEnabled(persona);
        if (getIsEnabled(existingPersonaEntity.getEntity()) != isEnabled) {
            if (isEnabled) {
                //TODO:enablePersona(existingPersonaWithExtInfo);
            } else {
                //TODO:disablePersona(existingPersonaWithExtInfo);
            }
        }

        /*if (!getName(persona).equals(getName(existingPersonaEntity))) {
            validateUniquenessByName(graph, getName(persona), PERSONA_ENTITY_TYPE);
        }*/

        //TODO: update role of required


        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName() {
        return getUUID();
    }

    private AtlasEntity.AtlasEntityWithExtInfo getAccessControlEntity(AtlasEntity entity, EntityMutations.EntityOperation op) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("AccessControlPolicyPreProcessor.getAccessControl");
        AtlasEntity.AtlasEntityWithExtInfo ret;

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(REL_ATTR_ACCESS_CONTROL);
        ret = entityRetriever.toAtlasEntityWithExtInfo(objectId);

        RequestContext.get().endMetricRecord(metricRecorder);
        return ret;
    }
}
