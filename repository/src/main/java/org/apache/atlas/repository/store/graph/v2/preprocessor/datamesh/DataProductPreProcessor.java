package org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

public class DataProductPreProcessor extends AbstractDomainPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DomainPreProcessor.class);
    private AtlasEntityHeader parentDomain;
    private EntityMutationContext context;
    public DataProductPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                   AtlasGraph graph, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph);
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (operation == EntityMutations.EntityOperation.CREATE && LOG.isDebugEnabled()) {
            LOG.debug("DataProductPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }
        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setParent(entity, context);

        switch (operation) {
            case CREATE:
                processCreateProduct(entity, vertex);
                break;
            case UPDATE:
                processUpdateProduct(entity, vertex);
                break;
        }
    }

    private void processCreateProduct(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateProduct");
        String productName = (String) entity.getAttribute(NAME);
        String parentDomainQualifiedName = (String) entity.getAttribute(PARENT_DOMAIN_QN);

        productExists(productName, parentDomainQualifiedName);
        String newQualifiedName = createQualifiedName(parentDomainQualifiedName);

        entity.setAttribute(QUALIFIED_NAME, newQualifiedName);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateProduct(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateProduct");
        String VertexQName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, VertexQName);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private static String createQualifiedName(String parentDomainQualifiedName) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(parentDomainQualifiedName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Parent Domain Qualified Name cannot be empty or null");
        }
        return parentDomainQualifiedName + "/product/" + getUUID();

    }

    private void setParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("DataProductPreProcessor.setParent");
        if (parentDomain == null) {
            Object relationshipAttribute = entity.getRelationshipAttribute(DATA_DOMAIN);
            Set<String> attributes = new HashSet<>(Arrays.asList(QUALIFIED_NAME, SUPER_DOMAIN_QN, PARENT_DOMAIN_QN, "__typeName"));

            if(relationshipAttribute instanceof AtlasObjectId){
                AtlasObjectId objectId = (AtlasObjectId) relationshipAttribute;
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
            else if(relationshipAttribute instanceof Map){
                Map<String, Object> relationshipMap = (Map<String, Object>) relationshipAttribute;
                if (StringUtils.isNotEmpty((String) relationshipMap.get("guid"))) {
                    AtlasVertex vertex = entityRetriever.getEntityVertex((String) relationshipMap.get("guid"));

                    if (vertex == null) {
                        parentDomain = entityRetriever.toAtlasEntityHeader((String) relationshipMap.get("guid"), attributes);
                    } else {
                        parentDomain = entityRetriever.toAtlasEntityHeader(vertex, attributes);
                    }

                }
                else  {
                    parentDomain = new AtlasEntityHeader((String) relationshipMap.get("typeName"), relationshipMap);

                }
            }
            else{
                LOG.warn("DataProductPreProcessor.setParent: Invalid relationshipAttribute {}", relationshipAttribute);
            }

        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void productExists(String productName, String parentDomainQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("domainExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", DATA_PRODUCT_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", productName)));


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

            List<AtlasEntityHeader> products = indexSearchPaginated(dsl, DATA_PRODUCT_ENTITY_TYPE);

            if (CollectionUtils.isNotEmpty(products)) {
                for (AtlasEntityHeader product : products) {
                    String name = (String) product.getAttribute(NAME);
                    if (productName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, productName+" already exists in the domain");
        }
    }

}