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
package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_POLICY_RESOURCES;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DomainPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    private EntityGraphMapper entityGraphMapper;
    private EntityMutationContext context;

    public DomainPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                              AtlasGraph graph, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph);
        this.entityGraphMapper = entityGraphMapper;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (operation == EntityMutations.EntityOperation.CREATE && LOG.isDebugEnabled()) {
            LOG.debug("DomainPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);

        if (operation == EntityMutations.EntityOperation.CREATE) {
            processCreateDomain(entity, vertex);
        }
    }

    private void processCreateDomain(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateDomain");
        String domainName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN);

        domainExists(domainName, parentDomainQualifiedName);
        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(parentDomainQualifiedName));

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName(String parentDomainQualifiedName) {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName) && parentDomainQualifiedName !=null) {
            return parentDomainQualifiedName + "/domain/" + getUUID();
        } else{
            String prefixQN = "default/domain";
            return prefixQN + "/" + getUUID();
        }
    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DomainPreProcessor.setParent");
        if (parentDomain == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(PARENT_DOMAIN);
            Set<String> attributes = new HashSet<>(Arrays.asList(QUALIFIED_NAME, SUPER_DOMAIN_QN, PARENT_DOMAIN_QN, "__typeName"));

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = entityRetriever.getEntityVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader(objectId.getGuid(), attributes);
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex, attributes);
                    }
                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty((String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    AtlasVertex parentDomainVertex = entityRetriever.getEntityVertex(objectId);
                    parentDomain = entityRetriever.toAtlasEntityHeader(parentDomainVertex, attributes);
                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void domainExists(String domainName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_DOMAIN_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", domainName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentDomain != null) {
                mustClauseList.add(mapOf("term", mapOf("parentDomainQualifiedName", parentDomainQualifiedName)));
            } else {
                List mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "parentDomainQualifiedName")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> domains = indexSearchPaginated(dsl, DATA_DOMAIN_ENTITY_TYPE);

            if (CollectionUtils.isNotEmpty(domains)) {
                for (AtlasEntityHeader domain : domains) {
                    String name = (String) domain.getAttribute(NAME);
                    if (domainName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, domainName+" already exists");
        }
    }
}