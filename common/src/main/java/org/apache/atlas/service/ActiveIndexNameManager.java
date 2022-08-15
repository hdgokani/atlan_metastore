package org.apache.atlas.service;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Order(Integer.MIN_VALUE)
@Component
public class ActiveIndexNameManager {

    private static String CURRENT_VERTEX_INDEX_NAME = "vertex_index";

    @PostConstruct
    public void init() {
        CURRENT_VERTEX_INDEX_NAME = "vertex_index";
    }

    public static String getCurrentIndexName() {
        return CURRENT_VERTEX_INDEX_NAME;
    }

    public static void setCurrentIndexName(String currentIndexName) {
        ActiveIndexNameManager.CURRENT_VERTEX_INDEX_NAME = currentIndexName;
    }
}
