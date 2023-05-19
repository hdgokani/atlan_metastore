package org.apache.atlas.repository.graph;

import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.RepositoryException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@Component
public class TypeCacheRefresher extends HostRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefresher.class);
    private final IAtlasGraphProvider provider;

    @Inject
    public TypeCacheRefresher(final IAtlasGraphProvider provider) {
        this.provider = provider;
    }

    public void refreshAllHostCache() throws IOException, URISyntaxException, RepositoryException {
        int totalFieldKeys = provider.get().getManagementSystem().getGraphIndex(VERTEX_INDEX).getFieldKeys().size();
        LOG.info("Found {} totalFieldKeys to be expected in other nodes :: traceId {}", totalFieldKeys, RequestContext.get().getTraceId());

        Map<String, String> params = new HashMap<>();
        params.put(HOST_REFRESH_TYPE_KEY, HostRefreshType.TYPE_DEFS.name());
        params.put("expectedFieldKeys", String.valueOf(totalFieldKeys));


        refreshCache(params, RequestContext.get().getTraceId());
    }
}