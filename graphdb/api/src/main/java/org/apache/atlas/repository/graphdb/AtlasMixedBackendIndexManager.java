package org.apache.atlas.repository.graphdb;

import java.io.IOException;

public interface AtlasMixedBackendIndexManager {

    void createIndexIfNotExists(String indexName) throws IOException;

    void deleteIndex(String indexName) throws IOException;
}
