package org.apache.atlas.repository.graphdb;

import java.io.IOException;

public interface AtlasIndexCreator {

    void createIndex(String indexName) throws IOException;
}
