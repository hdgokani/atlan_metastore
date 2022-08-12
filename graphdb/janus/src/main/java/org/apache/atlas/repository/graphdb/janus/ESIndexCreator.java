package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasIndexCreator;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.service.ActiveIndexNameManager.getCurrentIndexName;

@Component
public class ESIndexCreator implements AtlasIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(ESIndexCreator.class);

    @Override
    public void createIndex(String indexName) throws IOException {
        RestHighLevelClient esClient = AtlasElasticsearchDatabase.getClient();
        IndexTemplatesExistRequest indexTemplateExistsRequest = new IndexTemplatesExistRequest("atlan-template");
        boolean exists = false;
        try {
            exists = esClient.indices().existsTemplate(indexTemplateExistsRequest, RequestOptions.DEFAULT);
            if (exists) {
                LOG.info("atlan-template es index template exists!");
            } else {
                LOG.info("atlan-template es index template does not exists!");
            }
        } catch (Exception es) {
            LOG.error("Caught exception: {}", es.toString());
        }
        if (!exists) {
            createESIndex(esClient);
        }
    }

    private void createESIndex(RestHighLevelClient esClient) throws IOException {
        String vertexIndex = INDEX_PREFIX + getCurrentIndexName();
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("atlan-template");
        request.patterns(Collections.singletonList(vertexIndex));
        String atlasHomeDir = System.getProperty("atlas.home");

        Path elasticsearchSettingsFilePath = Paths.get(StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir, "elasticsearch", "es-settings.json");
        Settings requestSettings = Settings.builder().loadFromPath(elasticsearchSettingsFilePath).build();
        request.settings(requestSettings);
        try {
            AcknowledgedResponse putTemplateResponse = esClient.indices().putTemplate(request, RequestOptions.DEFAULT);
            if (putTemplateResponse.isAcknowledged()) {
                LOG.info("Atlan index template created.");
            } else {
                LOG.error("error creating atlan index template");
            }
        } catch (Exception e) {
            LOG.error("Caught exception: {}", e.toString());
            throw e;
        }
    }
}
