package org.apache.atlas.repository.graph;

import org.apache.atlas.RequestContext;
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

    public void refreshCache(boolean policies, boolean roles, boolean groups) {
        Map<String, String> params = new HashMap<>();
        params.put(HOST_REFRESH_TYPE_KEY, HostRefreshType.AUTH_CACHE.name());
        params.put("policies", String.valueOf(policies));
        params.put("roles", String.valueOf(roles));
        params.put("groups", String.valueOf(groups));

        try {
            refreshCache(params, RequestContext.get().getTraceId());
        } catch (IOException | URISyntaxException e) {
            LOG.warn("Failed to refresh authz cache on demand on other hosts: {}", e.getMessage());
        }
    }
}