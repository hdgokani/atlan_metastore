package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.*;


@Service
public class AtlasESIndexService implements AtlasRelationshipsIndexService {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasESIndexService.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasESIndexService");

    private final AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository;

    @Inject
    public AtlasESIndexService(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository) {
        this.atlasJanusVertexIndexRepository = atlasJanusVertexIndexRepository;
    }

    @Override
    public void createRelationships(List<AtlasRelationship> relationships) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "createRelationships()");
        }
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> createRelationships()");
            }
            if (CollectionUtils.isEmpty(relationships))
                return;
            Map<String, List<AtlasRelationship>> end1GuidRelationshipsMap = buildAssetToRelationshipsMap(relationships);
            for (String guid : end1GuidRelationshipsMap.keySet()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("==> creating relationships for guid: {}", guid);
                }
                UpdateByQueryRequest request = AtlasNestedRelationshipsQueryBuilder.getQueryForAppendingNestedRelationships(guid, end1GuidRelationshipsMap);
                atlasJanusVertexIndexRepository.updateByQuerySync(request, RequestOptions.DEFAULT);
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Override
    public void deleteRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        Objects.requireNonNull(relationship, "Relationship is null");
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "deleteRelationships()");
        }
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> deleteRelationship()");
            }
            UpdateByQueryRequest request = AtlasNestedRelationshipsQueryBuilder.getRelationshipDeletionQuery(relationship);
            atlasJanusVertexIndexRepository.updateByQuerySync(request, RequestOptions.DEFAULT);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private static Map<String, List<AtlasRelationship>> buildAssetToRelationshipsMap(List<AtlasRelationship> relationships) {
        Map<String, List<AtlasRelationship>> fromVertexGuidToRelationshipsMap = new HashMap<>();
        for (AtlasRelationship r : relationships) {
            String end1Guid = r.getEnd1().getGuid();
            List<AtlasRelationship> existingRelationshipsForGuid = fromVertexGuidToRelationshipsMap.getOrDefault(end1Guid, new ArrayList<>());
            existingRelationshipsForGuid.add(r);
            fromVertexGuidToRelationshipsMap.put(end1Guid, existingRelationshipsForGuid);
        }
        return fromVertexGuidToRelationshipsMap;
    }

}
