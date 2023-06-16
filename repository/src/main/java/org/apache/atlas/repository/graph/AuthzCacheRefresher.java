package org.apache.atlas.repository.graph;

import org.apache.atlas.RequestContext;
import org.apache.atlas.model.authcache.AuthzCacheRefreshInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


/*
* This class is responsible for notifying other Atlas hosts to
* update authz cache for on demand cache refresh request
* */
@Component
public class AuthzCacheRefresher extends HostRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzCacheRefresher.class);

    public void refreshCache(AuthzCacheRefreshInfo refreshInfo, String traceId) {
        Map<String, String> params = new HashMap<>();
        params.put(HOST_REFRESH_TYPE_KEY, HostRefreshType.AUTH_CACHE.name());
        params.put("refreshPolicies", String.valueOf(refreshInfo.isRefreshPolicies()));
        params.put("refreshRoles", String.valueOf(refreshInfo.isRefreshRoles()));
        params.put("refreshGroups", String.valueOf(refreshInfo.isRefreshGroups()));
        params.put("hardRefresh", String.valueOf(refreshInfo.isHardRefresh()));

        try {
            refreshCache(params, traceId);
        } catch (IOException | URISyntaxException e) {
            LOG.warn("Failed to refresh authz cache on demand on other hosts: {}", e.getMessage());
        }
    }
}