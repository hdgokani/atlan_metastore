package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

@Repository
public class AtlasJanusVertexIndexESRepositoryImpl implements AtlasJanusVertexIndexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusVertexIndexESRepositoryImpl.class);
    private final RestHighLevelClient elasticSearchClient = getClient();
    private final RestClient elasticSearchLowLevelClient = getLowLevelClient();
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_TIME_IN_MILLIS = 1000;

    // TODO: can async ES call
    @Override
    public UpdateResponse updateDoc(UpdateRequest request, RequestOptions options) throws AtlasBaseException {
        int count = 0;
        while(true) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Updating entity in ES with req {}", request.toString());
                }
                return elasticSearchClient.update(request, options);
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
                if (++count == MAX_RETRIES) throw new AtlasBaseException("All ES retries for relationships exhausted", e);
            }
        }
    }

    @Override
    public Response performRawRequest(String queryJson) throws AtlasBaseException {
        Objects.requireNonNull(queryJson, "query");
        int count = 0;
        while(true) {
            try {
                Request request = new Request(
                        "POST",
                        "/"+ INDEX_PREFIX + VERTEX_INDEX + "/_update_by_query");
                request.addParameters(Collections.emptyMap());
                HttpEntity entity = new StringEntity(queryJson, ContentType.APPLICATION_JSON);
                request.setEntity(entity);
                return elasticSearchLowLevelClient.performRequest(request);
            } catch (IOException e) {
                LOG.error(ExceptionUtils.getStackTrace(e));
                LOG.info("Retrying with delay of {} ms ", RETRY_TIME_IN_MILLIS);
                try {
                    Thread.sleep(RETRY_TIME_IN_MILLIS);
                } catch (InterruptedException ex) {
                    LOG.warn("Retry interrupted during ES relationship creation/deletion");
                    throw new AtlasBaseException("Retry interrupted during nested __relationship creation/deletion", ex);
                }
                if (++count == MAX_RETRIES) throw new AtlasBaseException("All ES retries for relationships exhausted", e);
            }
        }
    }
}