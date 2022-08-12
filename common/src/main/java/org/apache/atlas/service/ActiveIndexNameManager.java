package org.apache.atlas.service;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Order(Integer.MIN_VALUE)
@Component
public class ActiveIndexNameManager {

    private static String CURRENT_VERTEX_INDEX_NAME = "new_index_vertex";

    @PostConstruct
    public void init() {
        CURRENT_VERTEX_INDEX_NAME = "new_index_vertex";
    }

    public static String getCurrentIndexName() {
        return CURRENT_VERTEX_INDEX_NAME;
    }

    public void setCurrentIndexName(String currentIndexName) {
        ActiveIndexNameManager.CURRENT_VERTEX_INDEX_NAME = currentIndexName;
    }
}
