package org.apache.atlas.repository.graphdb.janus;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The requests will be executed in async manner,
 * this class provides all callback logic for responses
 * to handle failures and retries.
 * The listener provides methods to access to the response and the failure events.
 */
public class AtlasRelationshipIndexIOHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexIOHandler.class);

    public static ActionListener<UpdateResponse> getUpdateResponseListener(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository, UpdateRequest request) {

        return new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {}
            @Override
            public void onFailure(Exception e) {} // TODO: Handle retries
        };
    }

    public static ActionListener<BulkResponse> getBulkUpdateActionListener(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository, BulkRequest bulkRequest) {
        return new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                handleBulkResponseFailures(bulkResponse);
            }
            @Override
            public void onFailure(Exception e) {
                LOG.error("------- bulk update failed -------", e); // TODO: Retry handling
            }
        };
    }

    private ResponseListener getNewResponseListener(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {}
            @Override
            public void onFailure(Exception exception) {}
        };
    }

    private static void handleBulkResponseFailures(BulkResponse response) {
        for (BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure =
                        bulkItemResponse.getFailure();
                LOG.info("------- Update failed for id: {} -------", failure.getId());
            }
        }
    }

}
