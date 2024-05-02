package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
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

import java.util.Map;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataProductPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);

    private EntityGraphRetriever entityRetriever = null;
    private EntityGraphRetriever retrieverNoRelation = null;

    public DataProductPreProcessor(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.retrieverNoRelation = new EntityGraphRetriever(graph, typeRegistry, true);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        switch (operation) {
            case CREATE:
                processCreateProduct(entity);
                break;
            case UPDATE:
                processUpdateProduct(entity, vertex, context);
                break;
        }
    }

    private void processCreateProduct(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateProduct");
        AtlasObjectId parentDomainObject = (AtlasObjectId) entity.getRelationshipAttribute(PRODUCT_DOMAIN_REL_TYPE);

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

    private void processUpdateProduct(AtlasEntity entity, AtlasVertex vertex, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateProduct");

        AtlasEntity storedProduct = entityRetriever.toAtlasEntity(vertex);
        AtlasObjectId currentParent = (AtlasObjectId) storedProduct.getRelationshipAttribute(PRODUCT_DOMAIN_REL_TYPE);

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
                entity.getRelationshipAttributes().remove(PRODUCT_DOMAIN_REL_TYPE);
            }
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private AtlasEntityHeader getNewParentDomain(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("ProductPreProcessor.getNewParentDomain");

        try {
            Object parentDomainObject = entity.getRelationshipAttribute(PRODUCT_DOMAIN_REL_TYPE);
            if (parentDomainObject == null) {
                return null;
            }

            AtlasObjectId objectId;

            if (parentDomainObject instanceof Map) {
                objectId = getAtlasObjectIdFromMapObject(parentDomainObject);
            } else {
                objectId = (AtlasObjectId) parentDomainObject;
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