package org.apache.atlas.authorizer;

import org.apache.atlas.authorize.AtlasAccessRequest;

public class AccessResult {
    private boolean isAllowed = false;
    private String policyId;
    protected AtlasAccessRequest atlasAccessRequest;

    public boolean isAllowed() {
        return isAllowed;
    }

    public void setAllowed(boolean allowed) {
        this.isAllowed = allowed;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public AtlasAccessRequest getAtlasAccessRequest() {
        return atlasAccessRequest;
    }

    public void setAtlasAccessRequest(AtlasAccessRequest atlasAccessRequest) {
        this.atlasAccessRequest = atlasAccessRequest;
    }
}
