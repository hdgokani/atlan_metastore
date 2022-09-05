package org.apache.atlas.web.dto;

public class TypeSyncResponse {

    private final boolean shouldReindexData;
    private final boolean shouldCreateNewIndex;
    private final String currentIndexName;
    private final String newIndexName;

    public TypeSyncResponse(boolean shouldReindexData, boolean shouldCreateNewIndex, String currentIndexName, String newIndexName) {
        this.shouldReindexData = shouldReindexData;
        this.shouldCreateNewIndex = shouldCreateNewIndex;
        this.currentIndexName = currentIndexName;
        this.newIndexName = newIndexName;
    }

    public boolean isShouldReindexData() {
        return shouldReindexData;
    }

    public boolean isShouldCreateNewIndex() {
        return shouldCreateNewIndex;
    }

    public String getCurrentIndexName() {
        return currentIndexName;
    }

    public String getNewIndexName() {
        return newIndexName;
    }
}
