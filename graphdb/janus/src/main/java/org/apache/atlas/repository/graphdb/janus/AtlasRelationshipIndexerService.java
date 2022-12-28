package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.graphdb.janus.AtlasNestedRelationshipsESQueryBuilder.getQueryForAppendingNestedRelationships;
import static org.apache.atlas.repository.graphdb.janus.AtlasNestedRelationshipsESQueryBuilder.getRelationshipDeletionQuery;
import static org.apache.atlas.repository.graphdb.janus.AtlasRelationshipIndexResponseHandler.getBulkUpdateActionListener;
import static org.apache.atlas.repository.graphdb.janus.AtlasRelationshipIndexResponseHandler.getUpdateResponseListener;


@Service
public class AtlasRelationshipIndexerService implements AtlasRelationshipsService {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexerService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasRelationshipIndexerService");
    private static final String GUID_KEY = "__guid";
    private static final String END2_TYPENAME = "__typeName";
    private static final String RELATIONSHIPS_PARAMS_KEY = "relationships";
    private static final String RELATIONSHIP_GUID_KEY = "relationshipGuid";
    private static final String RELATIONSHIPS_TYPENAME_KEY = "relationshipTypeName";

    private final AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository;
    private final AtlasTypeRegistry typeRegistry;
    @Inject
    public AtlasRelationshipIndexerService(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository, AtlasTypeRegistry typeRegistry) {
        this.atlasJanusVertexIndexRepository = atlasJanusVertexIndexRepository;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void createRelationships(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> relationshipEndToVertexIdMap) {
        if (CollectionUtils.isEmpty(relationships) || MapUtils.isEmpty(relationshipEndToVertexIdMap))
            return;

        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "createRelationships()");

        try {
            if (LOG.isDebugEnabled())
                LOG.debug("==> createRelationships()");

            Map<String, List<AtlasRelationship>> vertexDocIdToRelationshipsMap = buildDocIdToRelationshipsMap(relationships, relationshipEndToVertexIdMap);
            for (String docId : vertexDocIdToRelationshipsMap.keySet()) {
                UpdateRequest request = getQueryForAppendingNestedRelationships(docId, getScriptParamsMap(vertexDocIdToRelationshipsMap, docId));
                atlasJanusVertexIndexRepository.updateDocAsync(request, RequestOptions.DEFAULT, getUpdateResponseListener(atlasJanusVertexIndexRepository, request));
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }



    @Override
    public void deleteRelationship(AtlasRelationship relationship, Map<AtlasObjectId, Object> end1ToVertexIdMap) throws AtlasBaseException {
        Objects.requireNonNull(relationship, "relationship");
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "deleteRelationships()");
        }
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("==> deleteRelationship()");

            final String docId = encodeVertexIdToESDocId(end1ToVertexIdMap, relationship);
            Map<String, Object> params = ImmutableMap.of(RELATIONSHIP_GUID_KEY, relationship.getGuid());
            UpdateRequest updateRequest = getRelationshipDeletionQuery(docId, params);
            UpdateResponse resp = atlasJanusVertexIndexRepository.updateDoc(updateRequest, RequestOptions.DEFAULT);
            if (LOG.isDebugEnabled())
                LOG.debug("==> ES update resp: {}", resp);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private void bulkUpdateRelationships(Map<String, List<AtlasRelationship>> vertexDocIdToRelationshipsMap) {
        BulkRequest request = new BulkRequest();
        for (String docId : vertexDocIdToRelationshipsMap.keySet()) {
            UpdateRequest updateRequest = getQueryForAppendingNestedRelationships(docId, getScriptParamsMap(vertexDocIdToRelationshipsMap, docId));
            request.add(updateRequest);
        }
        atlasJanusVertexIndexRepository.updateDocsInBulkAsync(request, getBulkUpdateActionListener(atlasJanusVertexIndexRepository));
    }

    private Map<String, List<AtlasRelationship>> buildDocIdToRelationshipsMap(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> endEntityToVertexIdMap) {
        Map<String, List<AtlasRelationship>> docIdToRelationshipsMap = new HashMap<>();
        for (AtlasRelationship r : relationships) {
            final String id = encodeVertexIdToESDocId(endEntityToVertexIdMap, r);
            List<AtlasRelationship> existingRelationshipsForDocId = docIdToRelationshipsMap.getOrDefault(id, new ArrayList<>());
            existingRelationshipsForDocId.add(r);
            docIdToRelationshipsMap.put(id, existingRelationshipsForDocId);
        }
        return docIdToRelationshipsMap;
    }

    private String encodeVertexIdToESDocId(Map<AtlasObjectId, Object> endToVertexIdMap, AtlasRelationship r) {
        AtlasRelationshipDef relationshipDef = this.typeRegistry.getRelationshipDefByName(r.getTypeName());
        Object vertex;
        if (relationshipDef.getEndDef1().getIsContainer()) {
            vertex = endToVertexIdMap.get(r.getEnd1());
        } else if (relationshipDef.getEndDef2().getIsContainer()) {
            vertex = endToVertexIdMap.get(r.getEnd2());
        } else {
            // Case: In case of ASSOCIATION, by default take end1 as container
            vertex = endToVertexIdMap.get(r.getEnd1());
        }
        Objects.requireNonNull(vertex);
        return LongEncoding.encode(Long.parseLong(vertex.toString()));
    }

    private static Map<String, Object> getScriptParamsMap(Map<String, List<AtlasRelationship>> end1DocIdRelationshipsMap, String docId) {
        Map<String, Object> paramsMap = new HashMap<>();
        List<Map<String, String>> relationshipNestedPayloadList = buildParamsListForScript(end1DocIdRelationshipsMap, docId);
        paramsMap.put(RELATIONSHIPS_PARAMS_KEY, relationshipNestedPayloadList);
        return paramsMap;
    }

    private static List<Map<String, String>> buildParamsListForScript(Map<String, List<AtlasRelationship>> end1DocIdRelationshipsMap, String docId) {
        List<Map<String, String>> relationshipNestedPayloadList = new ArrayList<>();
        for (AtlasRelationship r : end1DocIdRelationshipsMap.get(docId)) {
            Map<String, String> relationshipNestedPayload = buildNestedRelationshipDoc(r);
            relationshipNestedPayloadList.add(relationshipNestedPayload);
        }
        return relationshipNestedPayloadList;
    }

    private static Map<String, String> buildNestedRelationshipDoc(AtlasRelationship r) {
        return ImmutableMap.of(RELATIONSHIP_GUID_KEY, r.getGuid(), RELATIONSHIPS_TYPENAME_KEY, r.getTypeName(), GUID_KEY, r.getEnd2().getGuid(), END2_TYPENAME, r.getEnd2().getTypeName());
    }
}