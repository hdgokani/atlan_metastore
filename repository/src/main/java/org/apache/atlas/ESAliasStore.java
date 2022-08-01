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
import org.apache.atlas.purpose.PurposeContext;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.ESAliasRequestBuilder.ESAliasAction.ADD;
import static org.apache.atlas.persona.AtlasPersonaUtil.getAssets;
import static org.apache.atlas.persona.AtlasPersonaUtil.getConnectionId;
import static org.apache.atlas.persona.AtlasPersonaUtil.getGlossaryQualifiedNames;
import static org.apache.atlas.persona.AtlasPersonaUtil.getIsAllow;
import static org.apache.atlas.persona.AtlasPersonaUtil.getQualifiedName;
import static org.apache.atlas.persona.AtlasPersonaUtil.mapOf;
import static org.apache.atlas.purpose.AtlasPurposeUtil.getTags;
import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@Component
public class ESAliasStore implements IndexAliasStore {
    private static final Logger LOG = LoggerFactory.getLogger(ESAliasStore.class);

    private final AtlasGraph graph;
    private final EntityGraphRetriever entityRetriever;

    @Inject
    public ESAliasStore(AtlasGraph graph,
                        EntityGraphRetriever entityRetriever) {
        this.graph = graph;
        this.entityRetriever = entityRetriever;
    }

    @Override
    public boolean createAlias(PersonaContext personaContext) throws AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(PersonaContext personaContext) throws AtlasBaseException {
        String aliasName = getAliasName(personaContext.getPersona());

        Map<String, Object> filter = getFilter(personaContext);

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean createAlias(PurposeContext purposeContext) throws AtlasBaseException {
        String aliasName = getAliasName(purposeContext.getPurpose());

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName));

        graph.createOrUpdateESAlias(requestBuilder);
        return true;
    }

    @Override
    public boolean updateAlias(PurposeContext purposeContext) throws AtlasBaseException {
        String aliasName = getAliasName(purposeContext.getPurpose());

        Map<String, Object> filter = getFilter(purposeContext);

        ESAliasRequestBuilder requestBuilder = new ESAliasRequestBuilder();
        requestBuilder.addAction(ADD, new AliasAction(INDEX_PREFIX + VERTEX_INDEX, aliasName, filter));

        graph.createOrUpdateESAlias(requestBuilder);

        return true;
    }

    @Override
    public boolean deleteAlias(String aliasName) throws AtlasBaseException {
        graph.deleteESAlias(INDEX_PREFIX + VERTEX_INDEX, aliasName);
        return true;
    }

    private Map<String, Object> getFilter(PersonaContext context) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo personaEntityWithExtInfo = context.getPersonaExtInfo();

        Map<String, Object> ret = null;
        List<Map> allowClauseList = new ArrayList<>();
        List<Map> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = AtlasPersonaUtil.getMetadataPolicies(personaEntityWithExtInfo);

        if (CollectionUtils.isNotEmpty(policies)) {

            for (AtlasEntity entity: policies) {
                boolean addConnectionFilter = true;
                String connectionQName = getQualifiedName(getConnectionEntity(entity));

                List<String> assets = getAssets(entity);
                boolean allow = getIsAllow(entity);

                for (String asset : assets) {
                    if (StringUtils.equals(connectionQName, asset)) {
                        addConnectionFilter = false;
                    }

                    addPersonaMetadataFilterClauses(asset, allow ? allowClauseList : denyClauseList);
                }

                if (addConnectionFilter) {
                    addPersonaMetadataFilterConnectionClause(connectionQName, allow ? allowClauseList : denyClauseList);
                }
            }
        }

        policies = AtlasPersonaUtil.getGlossaryPolicies(personaEntityWithExtInfo);
        if (CollectionUtils.isNotEmpty(policies)) {
            for (AtlasEntity entity: policies) {
                List<String> glossaryQNames = getGlossaryQualifiedNames(entity);

                for (String glossaryQName : glossaryQNames) {
                    addPersonaGlossaryFilterClauses(glossaryQName, getIsAllow(entity) ? allowClauseList : denyClauseList);
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

    private Map<String, Object> getFilter(PurposeContext context) throws AtlasBaseException {
        AtlasEntity.AtlasEntityWithExtInfo purposeEntityWithExtInfo = context.getPurposeExtInfo();

        Map<String, Object> ret = null;
        List<Map> allowClauseList = new ArrayList<>();
        List<Map> denyClauseList = new ArrayList<>();

        List<AtlasEntity> policies = AtlasPersonaUtil.getMetadataPolicies(purposeEntityWithExtInfo);
        List<String> tags = getTags(context.getPurpose());

        if (CollectionUtils.isNotEmpty(policies)) {

            for (AtlasEntity entity: policies) {
                boolean allow = getIsAllow(entity);

                addPurposeMetadataFilterClauses(tags, allow ? allowClauseList : denyClauseList);
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

    private void addPersonaMetadataFilterClauses(String asset, List<Map> clauseList) {
        addPersonaFilterClauses(asset, clauseList);
    }

    private void addPersonaMetadataFilterConnectionClause(String connection, List<Map> clauseList) {
        clauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, connection)));
    }

    private void addPersonaGlossaryFilterClauses(String asset, List<Map> clauseList) {
        addPersonaFilterClauses(asset, clauseList);
    }

    private void addPersonaFilterClauses(String asset, List<Map> clauseList) {
        clauseList.add(mapOf("term", mapOf(QUALIFIED_NAME, asset)));
        clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, asset + "/*")));
        clauseList.add(mapOf("wildcard", mapOf(QUALIFIED_NAME, "*@" + asset)));
    }

    private void addPurposeMetadataFilterClauses(List<String> tags, List<Map> clauseList) {
        clauseList.add(mapOf("terms", mapOf(TRAIT_NAMES_PROPERTY_KEY, tags)));
        clauseList.add(mapOf("terms", mapOf(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, tags)));
    }

    private AtlasEntity getConnectionEntity(AtlasEntity personaPolicy) throws AtlasBaseException {
        String connectionId = getConnectionId(personaPolicy);

        return entityRetriever.toAtlasEntity(connectionId);
    }
}
