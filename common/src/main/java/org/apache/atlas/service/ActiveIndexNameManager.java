package org.apache.atlas.service;

public class ActiveIndexNameManager {

    public static final String DEFAULT_VERTEX_INDEX = "vertex_index";

    private static String CURRENT_READ_VERTEX_INDEX_NAME = "vertex_index";
    private static String CURRENT_WRITE_VERTEX_INDEX_NAME = "vertex_index";

    public static void init(String indexName) {
        setCurrentReadVertexIndexName(indexName);
        setCurrentWriteVertexIndexName(indexName);
    }

    public static String getCurrentReadVertexIndexName() {
        return CURRENT_READ_VERTEX_INDEX_NAME;
    }

    public static void setCurrentReadVertexIndexName(String currentIndexName) {
        CURRENT_READ_VERTEX_INDEX_NAME = currentIndexName;
    }

    public static String getCurrentWriteVertexIndexName() {
        return CURRENT_WRITE_VERTEX_INDEX_NAME;
    }

    public static void setCurrentWriteVertexIndexName(String currentWriteVertexIndexName) {
        CURRENT_WRITE_VERTEX_INDEX_NAME = currentWriteVertexIndexName;
    }

    public static String getNewIndexName(long TS) {
        return "vertex_index" + "_" + TS;
    }
}
