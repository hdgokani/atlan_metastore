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
package org.apache.atlas.transformer;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.repository.store.graph.v2.preprocessor.ConnectionPreProcessor;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.getStaticFileAsString;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_ROLES;
import static org.apache.atlas.util.AtlasEntityUtils.*;

public class ConnectionPoliciesTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPreProcessor.class);

    static final String TEMPLATE_FILE_NAME = "templates/connection_bootstrap_policies.json";
    static final String PLACEHOLDER_ENTITY = "{entity}";
    static final String PLACEHOLDER_CONN_GUID = "{connection_guid}";
    static final String PLACEHOLDER_CONN_NAME = "{connection_name}";

    static List<AtlasEntity> connectionPolicies = new ArrayList<>();

    static {
        //connectionPolicies
        //File jsonTemplateFile = new File(ConnectionPoliciesTransformer.class.getResource(TEMPLATE_FILE_NAME).getPath());
        String jsonTemplate = null;
        try {
            jsonTemplate = getStaticFileAsString(TEMPLATE_FILE_NAME);
        } catch (IOException e) {
            LOG.error("Failed to load template for connection policies: {}", TEMPLATE_FILE_NAME);
        }

        AtlasEntity[] entities = AtlasType.fromJson(jsonTemplate, AtlasEntity[].class);
        connectionPolicies = Arrays.asList(entities);
    }

    public ConnectionPoliciesTransformer() {
        connectionPolicies.size();
    }

    public AtlasEntitiesWithExtInfo transform(AtlasEntity connection, String roleName) {
        LOG.info("transforming connection bootstrap policies");
        String qualifiedName = getQualifiedName(connection);
        String guid = connection.getGuid();
        String name = getName(connection);

        AtlasEntitiesWithExtInfo policiesExtInfo = new AtlasEntitiesWithExtInfo();

        for (AtlasEntity bootPolicy : connectionPolicies) {
            String bootPolicyName = getName(bootPolicy);
            String bootPolicyQn = getQualifiedName(bootPolicy);

            bootPolicy.setAttribute(NAME, bootPolicyName.replace(PLACEHOLDER_CONN_NAME, name));
            bootPolicy.setAttribute(QUALIFIED_NAME, bootPolicyQn.replace(PLACEHOLDER_CONN_GUID, guid));

            bootPolicy.setAttribute(ATTR_POLICY_ROLES, Arrays.asList(roleName));

            List<String> resources = getListAttribute(bootPolicy, ATTR_POLICY_RESOURCES);

            List<String> resourcesFinal  = new ArrayList<>();
            for (String resource : resources) {
                if (resource.contains(PLACEHOLDER_ENTITY)) {
                    resource = resource.replace(PLACEHOLDER_ENTITY, qualifiedName);
                }

                resourcesFinal.add(resource);
            }
            bootPolicy.setAttribute(ATTR_POLICY_RESOURCES, resourcesFinal);

            policiesExtInfo.addEntity(bootPolicy);
            LOG.info("transformed connection bootstrap policies");
        }

        return policiesExtInfo;
    }
}
