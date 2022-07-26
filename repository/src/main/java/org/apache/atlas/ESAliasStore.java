/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas;

import org.apache.atlas.ESAliasRequestBuilder.AliasAction;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.persona.AtlasPersonaUtil;
import org.apache.atlas.persona.PersonaContext;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.collections.CollectionUtils;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.persona.AtlasPersonaUtil.getAssets;
import static org.apache.atlas.persona.AtlasPersonaUtil.getGlossaryQualifiedNames;
import static org.apache.atlas.persona.AtlasPersonaUtil.getIsAllow;
import static org.apache.atlas.persona.AtlasPersonaUtil.mapOf;
import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@Component
public class ESAliasStore implements IndexAliasStore {
    private static final Logger LOG = LoggerFactory.getLogger(ESAliasStore.class);

    private final AtlasGraph graph;

    @Inject
    public ESAliasStore(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public boolean createAlias(PersonaContext personaContext) throws JSONException, IOException, AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        Map<String, Object> filter = getFilter(personaContext.getPersonaExtInfo());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(PersonaContext personaContext) throws JSONException, IOException, AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        Map<String, Object> filter = getFilter(personaContext.getPersonaExtInfo());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean deleteAlias(PersonaContext personaContext) throws JSONException, IOException, AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        graph.deleteESAlias(INDEX_PREFIX + VERTEX_INDEX, aliasName);
        return true;
    }

    private Map<String, Object> getFilter(AtlasEntity.AtlasEntityWithExtInfo personaEntityWithExtInfo) {
        Map<String, Object> ret = null;
        List<Map> allowClauseList = new ArrayList<>();
        List<Map> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = AtlasPersonaUtil.getMetadataPolicies(personaEntityWithExtInfo);

        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity entity: policies) {
                List<String> assets = getAssets(entity);

                if (getIsAllow(entity)) {
                    for (String asset : assets) {
                        allowClauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, asset)));
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
                    }
                } else {
                    for (String asset : assets) {
                        denyClauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, asset)));
                        denyClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
                    }
                }
            }
        }

        policies = AtlasPersonaUtil.getGlossaryPolicies(personaEntityWithExtInfo);
        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity entity: policies) {
                List<String> glossaryQnames = getGlossaryQualifiedNames(entity);

                if (getIsAllow(entity)) {
                    for (String glossaryQName : glossaryQnames) {
                        allowClauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, glossaryQName)));
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, glossaryQName + "/*")));
                        allowClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, "*@" + glossaryQName)));
                    }
                } else {
                    for (String glossaryQName : glossaryQnames) {
                        denyClauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, glossaryQName)));
                        denyClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, glossaryQName + "/*")));
                        denyClauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, "*@" + glossaryQName)));
                    }
                }
            }
        }

        Map bool = new HashMap();
        if (CollectionUtils.isNotEmpty(allowClauseList)) {
            bool.put("should", allowClauseList);
        }

        if (CollectionUtils.isNotEmpty(denyClauseList)) {
            bool.put("must_not", denyClauseList);
        }

        ret = mapOf("bool", bool);

        return ret;
    }

    private String getAliasName(AtlasEntity persona) {
        String qualifiedName = AtlasPersonaUtil.getQualifiedName(persona);

        String[] parts = qualifiedName.split("/");

        return parts[parts.length - 1];
    }
}
