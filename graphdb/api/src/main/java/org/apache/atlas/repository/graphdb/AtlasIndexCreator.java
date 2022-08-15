package org.apache.atlas.repository.graphdb;

import java.io.IOException;

public interface AtlasIndexCreator {

    void createIndexIfNotExists(String indexName) throws IOException;

}
