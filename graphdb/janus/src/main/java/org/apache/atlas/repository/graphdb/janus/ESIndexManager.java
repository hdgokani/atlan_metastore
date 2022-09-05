package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasMixedBackendIndexManager;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;

@Component
public class ESIndexManager implements AtlasMixedBackendIndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(ESIndexManager.class);

    private final RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();

    @Override
    public void createIndexIfNotExists(String indexName) throws IOException {
        if (!doesIndexExist(indexName)) {
            doCreateIndex(indexName);
        }
    }

    private boolean doesIndexExist(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_PREFIX + indexName);
        try {
            return esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Error sending index exists request: ", e);
            throw e;
        }
    }

    private void doCreateIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_PREFIX + indexName);
        try {
            AcknowledgedResponse response = esClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                LOG.info("Atlan index {} created.", INDEX_PREFIX + indexName);
            } else {
                LOG.error("error creating atlan index {}", INDEX_PREFIX + indexName);
            }
        } catch (Exception e) {
            LOG.error("Caught exception: {}", e.toString());
            throw e;
        }
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(INDEX_PREFIX + indexName);
        try {
            AcknowledgedResponse response = esClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                LOG.info("Atlan index {} deleted.", INDEX_PREFIX + indexName);
            } else {
                LOG.error("error deleting atlan index {}", INDEX_PREFIX + indexName);
            }
        } catch (Exception e) {
            LOG.error("Caught exception: {}", e.toString());
            throw e;
        }

    }
}
