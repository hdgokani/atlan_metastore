package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;

public interface AtlasJanusVertexIndexRepository {

    BulkByScrollResponse updateByQuerySync(UpdateByQueryRequest request, RequestOptions options) throws AtlasBaseException;
    void updateByQueryAsync(UpdateByQueryRequest request, RequestOptions options, ActionListener<BulkByScrollResponse> listener) throws AtlasBaseException;
}
