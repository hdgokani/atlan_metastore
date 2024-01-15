package org.apache.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessRequest;
import org.apache.atlas.plugin.model.RangerPolicy;

public class AccessResult {
    private boolean isAllowed = false;
    private RangerPolicy rangerPolicy;
    protected AtlasAccessRequest atlasAccessRequest;

    public boolean isAllowed() {
        return isAllowed;
    }

    public void setAllowed(boolean allowed) {
        this.isAllowed = allowed;
    }

    public RangerPolicy getRangerPolicy() {
        return rangerPolicy;
    }

    public void setRangerPolicy(RangerPolicy rangerPolicy) {
        this.rangerPolicy = rangerPolicy;
    }

    public AtlasAccessRequest getAtlasAccessRequest() {
        return atlasAccessRequest;
    }

    public void setAtlasAccessRequest(AtlasAccessRequest atlasAccessRequest) {
        this.atlasAccessRequest = atlasAccessRequest;
    }
}
