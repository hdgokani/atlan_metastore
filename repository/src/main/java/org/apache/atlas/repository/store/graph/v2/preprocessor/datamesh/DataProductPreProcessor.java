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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class DataProductPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DataProductPreProcessor.class);

    private EntityGraphRetriever retriever = null;

    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, AtlasGraph graph) {
        this.retriever = new EntityGraphRetriever(graph, typeRegistry, true);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateProduct(entity);
                break;
            case UPDATE:
                processUpdateProduct(entity);
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
            AtlasVertex parentDomain = retriever.getEntityVertex(parentDomainObject);
            String parentDomainQualifiedName = parentDomain.getProperty(QUALIFIED_NAME, String.class);

            entity.setAttribute(PARENT_DOMAIN_QN, parentDomainQualifiedName);

            String[] splitted = parentDomainQualifiedName.split("/");
            String superDomainQualifiedName = String.format("%s/%s/%s", splitted[0], splitted[1], splitted[2]);
            entity.setAttribute(SUPER_DOMAIN_QN, superDomainQualifiedName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateProduct(AtlasEntity entity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateProduct");

        //never update these attributes with API request
        entity.removeAttribute(PARENT_DOMAIN_QN);
        entity.removeAttribute(SUPER_DOMAIN_QN);
        if (entity.getRelationshipAttributes() != null) {
            entity.getRelationshipAttributes().remove(PRODUCT_DOMAIN_REL_TYPE);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }
}