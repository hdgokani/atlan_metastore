package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;

public interface AtlasJanusVertexIndexRepository {

    BulkResponse performBulkAsync(BulkRequest bulkRequest) throws AtlasBaseException;

    UpdateResponse updateDoc(UpdateRequest request, RequestOptions options) throws AtlasBaseException;

    void updateDocAsync(UpdateRequest request, RequestOptions options, ActionListener<UpdateResponse> listener);

    void updateDocsInBulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    Response performRawRequest(String query, String docId) throws AtlasBaseException;

    void performRawRequestAsync(String query, String docId, ResponseListener listener);

}
