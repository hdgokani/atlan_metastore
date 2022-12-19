package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import java.io.IOException;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getClient;

@Repository
public class AtlasJanusVertexIndexESRepositoryImpl implements AtlasJanusVertexIndexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusVertexIndexESRepositoryImpl.class);
    private final RestHighLevelClient elasticSearchClient = getClient();
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_TIME_IN_MILLIS = 1000;

    @Override
    public BulkByScrollResponse updateByQuerySync(UpdateByQueryRequest request, RequestOptions options) throws AtlasBaseException {
        int count = 0;
        while(true) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating entity in ES with req {}", request.toString());
                }
                return elasticSearchClient.updateByQuery(request, options);
            } catch (IOException e) {
                LOG.warn(String.format("Exception while trying to create nested relationship for req %s. Retrying",
                        request.toString()), e);
                LOG.info("Retrying with delay of {} ms ", RETRY_TIME_IN_MILLIS);
                try {
                    Thread.sleep(RETRY_TIME_IN_MILLIS);
                } catch (InterruptedException ex) {
                    LOG.warn("Retry interrupted during edge creation ");
                    throw new AtlasBaseException("Retry interrupted during nested __relationship creation", ex);
                }
                if (++count == MAX_RETRIES) throw new AtlasBaseException(e);
            }
        }
    }

    // TODO: implement async flow
    @Override
    public void updateByQueryAsync(UpdateByQueryRequest request, RequestOptions options, ActionListener<BulkByScrollResponse> listener) throws AtlasBaseException {

    }
}