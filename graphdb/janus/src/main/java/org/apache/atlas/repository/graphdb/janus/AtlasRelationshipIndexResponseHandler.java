package org.apache.atlas.repository.graphdb.janus;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AtlasRelationshipIndexResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexResponseHandler.class);

    public static ActionListener<UpdateResponse> getUpdateResponseListener(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository, UpdateRequest request) {

        return new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                //LOG.info("------- async update response received for id: {} -------", updateResponse.getId());
            }
            @Override
            public void onFailure(Exception e) {
                LOG.info("------- async update response failed -------");
                // TODO: Handle retries
            }
        };
    }

    public static ActionListener<BulkResponse> getBulkUpdateActionListener(AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository) {
        return new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                //LOG.info("------- bulk update response received: {} -------", bulkResponse.getItems().length);
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
            public void onSuccess(Response response) {
                try {
                    LOG.info("------- async raw update response {} -------", EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void onFailure(Exception exception) {
                LOG.error("------- async raw update response failed ------- ", exception);
                // TODO: Handle retries
            }
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
