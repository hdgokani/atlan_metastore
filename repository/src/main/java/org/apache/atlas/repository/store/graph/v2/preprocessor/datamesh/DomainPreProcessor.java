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


import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DomainPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private EntityGraphRetriever entityRetriever = null;
    private EntityGraphRetriever retrieverNoRelation = null;

    public DomainPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.retrieverNoRelation = new EntityGraphRetriever(graph, typeRegistry, true);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("DomainPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateDomain(entity);
                break;
            case UPDATE:
                processUpdateDomain(entity, vertex, context);
                break;
        }
    }

    private void processCreateDomain(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateDomain");
        AtlasObjectId parentDomainObject = (AtlasObjectId) entity.getRelationshipAttribute(DOMAIN_DOMAIN_PARENT_REL_TYPE);

        if (parentDomainObject == null) {
            entity.removeAttribute(PARENT_DOMAIN_QN);
            entity.removeAttribute(SUPER_DOMAIN_QN);
        } else {
            AtlasVertex parentDomain = retrieverNoRelation.getEntityVertex(parentDomainObject);
            String parentDomainQualifiedName = parentDomain.getProperty(QUALIFIED_NAME, String.class);

            entity.setAttribute(PARENT_DOMAIN_QN, parentDomainQualifiedName);

            String[] splitted = parentDomainQualifiedName.split("/");
            String superDomainQualifiedName = String.format("%s/%s/%s", splitted[0], splitted[1], splitted[2]);
            entity.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateDomain(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateDomain");

        AtlasEntity storedDomain = entityRetriever.toAtlasEntity(vertex);
        AtlasObjectId currentParent = (AtlasObjectId) storedDomain.getRelationshipAttribute(DOMAIN_DOMAIN_PARENT_REL_TYPE);

        AtlasEntityHeader newParent = getNewParentDomain(entity, context);

        if (currentParent == null && newParent != null) {
            // attaching parent for the first time, set attributes as well

            String parentDomainQualifiedName = (String) newParent.getAttribute(QUALIFIED_NAME);
            entity.setAttribute(PARENT_DOMAIN_QN, parentDomainQualifiedName);

            String superDomainQualifiedName = (String) newParent.getAttribute(SUPER_DOMAIN_QN);
            if (StringUtils.isEmpty(superDomainQualifiedName)) {
                String[] splitted = parentDomainQualifiedName.split("/");
                superDomainQualifiedName = String.format("%s/%s/%s", splitted[0], splitted[1], splitted[2]);
            }

            entity.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);

        } else {
            //never update these attributes with API request
            entity.removeAttribute(PARENT_DOMAIN_QN);
            entity.removeAttribute(SUPER_DOMAIN_QN);
            if (entity.getRelationshipAttributes() != null) {
                entity.getRelationshipAttributes().remove(DOMAIN_DOMAIN_PARENT_REL_TYPE);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private AtlasEntityHeader getNewParentDomain(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
    AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DomainPreProcessor.getNewParentDomain");

        AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(DOMAIN_DOMAIN_PARENT_REL_TYPE);

        try {
            if (objectId == null) {
                return null;
            }

            if (StringUtils.isNotEmpty(objectId.getGuid())) {
                AtlasVertex vertex = context.getVertex(objectId.getGuid());

                if (vertex == null) {
                    return retrieverNoRelation.toAtlasEntityHeader(objectId.getGuid());
                } else {
                    return retrieverNoRelation.toAtlasEntityHeader(vertex);
                }

            } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty((String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                return new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        return null;
    }
}