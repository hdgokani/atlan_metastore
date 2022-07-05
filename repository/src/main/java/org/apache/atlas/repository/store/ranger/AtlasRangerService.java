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
package org.apache.atlas.repository.store.ranger;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;


@Component
public class AtlasRangerService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRangerService.class);

    static final boolean RANGER_USERNAME = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
    static final boolean RANGER_PASSWORD = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();

    private final RangerAdminRESTClient rangerAdminRESTClient;
    private final AtlasEntityStore entityStore;

    @Inject
    public AtlasRangerService(AtlasEntityStore entityStore) {
        this.entityStore = entityStore;
        this.rangerAdminRESTClient = new RangerAdminRESTClient();
    }

    public void createRangerRole(String roleName, String roleDescription) {

        try {
            RangerRole rangerRole = new RangerRole();
            rangerRole.setName(roleName);
            rangerRole.setDescription(roleDescription);

            rangerAdminRESTClient.createRole(rangerRole);


        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }
}
