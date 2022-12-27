package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;

public interface AtlasJanusVertexIndexRepository {

    BulkResponse updateDocsInBulk(BulkRequest bulkRequest) throws AtlasBaseException;
    UpdateResponse updateDoc(UpdateRequest request, RequestOptions options) throws AtlasBaseException;
    Response performRawRequest(String query, String docId) throws AtlasBaseException;
}
