package org.apache.atlas.repository.graphdb.janus;

import com.google.common.collect.ImmutableMap;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;


@Service
public class AtlasRelationshipIndexerService implements AtlasRelationshipsService {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexerService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasESIndexService");
    private static final String GUID_KEY = "__guid";
    private static final String END2_TYPENAME = "__typeName";
    private static final String RELATIONSHIPS_PARAMS_KEY = "relationships";
    private static final String RELATIONSHIP_GUID_KEY = "relationshipGuid";
    private static final String RELATIONSHIPS_TYPENAME_KEY = "relationshipTypeName";

    private final AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository;

    @Inject
    public AtlasRelationshipIndexerService(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository) {
        this.atlasJanusVertexIndexRepository = atlasJanusVertexIndexRepository;
    }

    @Override
    public void createRelationships(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> end1ToVertexIdMap) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(relationships) || MapUtils.isEmpty(end1ToVertexIdMap))
            return;

        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "createRelationships()");

        try {
            if (LOG.isDebugEnabled())
                LOG.debug("==> createRelationships()");

            Map<String, List<AtlasRelationship>> end1DocIdToRelationshipsMap = buildDocIdToRelationshipsMap(relationships, end1ToVertexIdMap);
            for (String docId : end1DocIdToRelationshipsMap.keySet()) {
                if (LOG.isDebugEnabled())
                    LOG.debug("==> creating relationships for ES _id: {}", docId);
                String json = AtlasNestedRelationshipsESQueryBuilder.getJsonQueryForAppendingNestedRelationships(getScriptParamsMap(end1DocIdToRelationshipsMap, docId));
                Response resp = atlasJanusVertexIndexRepository.performRawRequest(json, docId);
                LOG.info("ES _update resp -------------------> {}", EntityUtils.toString(resp.getEntity()));
            }
        } catch (IOException e) {
            throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, e);
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
            UpdateRequest updateRequest = AtlasNestedRelationshipsESQueryBuilder.getRelationshipDeletionQuery(relationship, docId, params);
            UpdateResponse resp = atlasJanusVertexIndexRepository.updateDoc(updateRequest, RequestOptions.DEFAULT);
            if (LOG.isDebugEnabled())
                LOG.debug("==> ES update resp: {}", resp);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private static Map<String, List<AtlasRelationship>> buildDocIdToRelationshipsMap(List<AtlasRelationship> relationships, Map<AtlasObjectId, Object> end1ToVertexIdMap) {
        Map<String, List<AtlasRelationship>> docIdToRelationshipsMap = new HashMap<>();
        for (AtlasRelationship r : relationships) {
            final String id = encodeVertexIdToESDocId(end1ToVertexIdMap, r);
            List<AtlasRelationship> existingRelationshipsForDocId = docIdToRelationshipsMap.getOrDefault(id, new ArrayList<>());
            existingRelationshipsForDocId.add(r);
            docIdToRelationshipsMap.put(id, existingRelationshipsForDocId);
        }
        return docIdToRelationshipsMap;
    }

    private static String encodeVertexIdToESDocId(Map<AtlasObjectId, Object> end1ToVertexIdMap, AtlasRelationship r) {
        Object end1VertexId = end1ToVertexIdMap.get(r.getEnd1());
        Objects.requireNonNull(end1VertexId);
        return LongEncoding.encode(Long.parseLong(end1VertexId.toString()));
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