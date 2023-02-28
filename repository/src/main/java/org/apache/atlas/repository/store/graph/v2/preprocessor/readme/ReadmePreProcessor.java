package org.apache.atlas.repository.store.graph.v2.preprocessor.readme;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.RestoreHandlerV1;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.GlossaryPreProcessor;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;

public class ReadmePreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryPreProcessor.class);

    private static AtlasTypeRegistry typeRegistry;
    private EntityGraphRetriever entityRetriever;
    private static AtlasGraph graph;

    public ReadmePreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever, AtlasGraph graph) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
        this.graph = graph;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("GlossaryPreProcessor.processAttributes: pre processing {}, {}", entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        AtlasEntity entity = (AtlasEntity) entityStruct;

        switch (operation) {
            case CREATE:
                processCreateReadme(entity, context);
                break;
            case UPDATE:
                processUpdateReadme(entity, context.getVertex(entity.getGuid()));
                break;
        }
    }

    private void processCreateReadme(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        RequestContext requestContext = RequestContext.get();
        AtlasPerfMetrics.MetricRecorder metricRecorder = requestContext.startMetricRecord("processCreateReadme");
        String entityQualifiedName = createQualifiedName(entity);

        entity.setAttribute(QUALIFIED_NAME, entityQualifiedName);

        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());
        AtlasVertex vertex = AtlasGraphUtilsV2.findByUniqueAttributes(graph, entityType, entity.getAttributes());
        if(vertex != null && !vertex.getProperty(STATE_PROPERTY_KEY, String.class).equals(DELETED.name())){
            String guidOfExistingReadme = GraphHelper.getGuid(vertex);
            entity.setGuid(guidOfExistingReadme);
            context.cacheEntity(guidOfExistingReadme, vertex, entityType);
        }
        else if(vertex != null){
            RestoreHandlerV1 restoreHandlerV1 = new RestoreHandlerV1(graph, typeRegistry);
            GraphTransactionInterceptor.addToVertexStateCache(vertex.getId(), AtlasEntity.Status.ACTIVE);
            restoreHandlerV1.restoreEntities(Collections.singletonList(vertex));
            context.cacheEntity(entity.getGuid(), vertex, entityType);
        }
        AtlasEntityHeader header = new AtlasEntityHeader(entity);
        requestContext.recordEntityUpdate(header);
        requestContext.cacheDifferentialEntity(entity);
        requestContext.endMetricRecord(metricRecorder);
    }
    private void processUpdateReadme(AtlasEntity entity, AtlasVertex vertex) {
        RequestContext requestContext = RequestContext.get();
        AtlasPerfMetrics.MetricRecorder metricRecorder = requestContext.startMetricRecord("processUpdateReadme");
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        AtlasEntityHeader header = new AtlasEntityHeader(entity);
        requestContext.recordEntityUpdate(header);
        requestContext.cacheDifferentialEntity(entity);
        requestContext.endMetricRecord(metricRecorder);
    }

    public static String createQualifiedName(AtlasEntity entity) throws AtlasBaseException {
        AtlasObjectId asset = (AtlasObjectId) entity.getRelationshipAttribute("asset");
        String guid = asset.getGuid();
        if(guid == null)
            guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(graph, typeRegistry.getEntityTypeByName(asset.getTypeName()), asset.getUniqueAttributes());

        return guid + "/readme";
    }
}