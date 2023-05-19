/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.AtlasErrorCode.CINV_UNHEALTHY;

public abstract class HostRefresher {
    private static final Logger LOG = LoggerFactory.getLogger(HostRefresher.class);

    String cacheRefresherEndpoint;
    String cacheRefresherHealthEndpoint;
    boolean isActiveActiveHAEnabled;

    public static final String HOST_REFRESH_TYPE_KEY = "hostRefreshType";

    public enum HostRefreshType {
        TYPE_DEFS,
        AUTH_CACHE
    }

    @PostConstruct
    public void init() throws AtlasException {
        Configuration configuration = ApplicationProperties.get();
        this.cacheRefresherEndpoint = configuration.getString("atlas.server.type.cache-refresher");
        this.cacheRefresherHealthEndpoint = configuration.getString("atlas.server.type.cache-refresher-health");
        this.isActiveActiveHAEnabled = HAConfiguration.isActiveActiveHAEnabled(configuration);
        LOG.info("Found {} as cache-refresher endpoint", cacheRefresherEndpoint);
        LOG.info("Found {} as cache-refresher-health endpoint", cacheRefresherHealthEndpoint);
    }

    public void verifyCacheRefresherHealth() throws AtlasBaseException, IOException {
        if (StringUtils.isBlank(cacheRefresherHealthEndpoint) || !isActiveActiveHAEnabled) {
            LOG.info("Skipping type-def cache refresher health checking as URL is {} and isActiveActiveHAEnabled is {}", cacheRefresherHealthEndpoint, isActiveActiveHAEnabled);
            return;
        }
        final String healthResponseBody;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            final HttpGet healthRequest = new HttpGet(cacheRefresherHealthEndpoint);
            healthResponseBody = executeGet(client, healthRequest);
        }
        LOG.debug("Response Body from cache-refresh-health = {}", healthResponseBody);
        final ObjectMapper mapper = new ObjectMapper();
        final CacheRefresherHealthResponse jsonResponse = mapper.readValue(healthResponseBody, CacheRefresherHealthResponse.class);
        if (!"Healthy".equalsIgnoreCase(jsonResponse.getMessage())) {
            throw new AtlasBaseException(CINV_UNHEALTHY);
        }
    }

    void refreshCache(Map<String, String> params, String traceId) throws IOException, URISyntaxException {
        if(StringUtils.isBlank(cacheRefresherEndpoint) || !isActiveActiveHAEnabled) {
            LOG.info("Skipping cache refresh :: traceId {}", traceId);
            return;
        }

        URIBuilder builder = new URIBuilder(cacheRefresherEndpoint);

        if (MapUtils.isNotEmpty(params)) {
            for (String key : params.keySet()) {
                builder.setParameter(key, params.get(key));
            }
        }
        builder.setParameter("traceId", traceId);

        final HttpPost httpPost = new HttpPost(builder.build());
        LOG.info("Invoking cache refresh endpoint {} :: traceId {}", cacheRefresherEndpoint, traceId);

        String responseBody;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            responseBody = executePost(traceId, client, httpPost);
        }
        LOG.info("Response Body from cache-refresh = {} :: traceId {}", responseBody, traceId);
        CacheRefreshResponseEnvelope cacheRefreshResponseEnvelope = convertStringToObject(responseBody);

        for (CacheRefreshResponse responseOfEachNode : cacheRefreshResponseEnvelope.getResponse()) {
            if (responseOfEachNode.getStatus() != 204) {
                //Do not throw exception in this case as node must have been in passive state now
                LOG.error("Error while performing cache refresh on host {} . HTTP code = {} :: traceId {}", responseOfEachNode.getHost(),
                        responseOfEachNode.getStatus(), traceId);
            } else {
                LOG.info("Host {} returns response code {} :: traceId {}", responseOfEachNode.getHost(), responseOfEachNode.getStatus(), traceId);
            }
        }
        LOG.info("Refreshed cache successfully on all hosts :: traceId {}", traceId);
    }

    private String executePost(String traceId, CloseableHttpClient client, HttpPost httpPost) throws IOException {
        try (CloseableHttpResponse response = client.execute(httpPost)) {
            LOG.info("Received HTTP response code {} from cache refresh endpoint :: traceId {}", response.getStatusLine().getStatusCode(), traceId);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Error while calling cache-refresher on host " + cacheRefresherEndpoint + ". HTTP code = " + response.getStatusLine().getStatusCode() + " :: traceId " + traceId);
            }
            return EntityUtils.toString(response.getEntity());
        }
    }

    private String executeGet(CloseableHttpClient client, HttpGet getRequest) throws IOException, AtlasBaseException {
        try (CloseableHttpResponse closeableHttpResponse = client.execute(getRequest)) {
            LOG.info("Received HTTP response code {} from cache refresh health endpoint", closeableHttpResponse.getStatusLine().getStatusCode());
            if (closeableHttpResponse.getStatusLine().getStatusCode() != 200) {
                throw new AtlasBaseException(CINV_UNHEALTHY);
            }
            return EntityUtils.toString(closeableHttpResponse.getEntity());
        }
    }

    private CacheRefreshResponseEnvelope convertStringToObject(final String responseBody) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(responseBody, CacheRefreshResponseEnvelope.class);
    }
}

class CacheRefreshResponseEnvelope {
    private List<CacheRefreshResponse> response;

    public List<CacheRefreshResponse> getResponse() {
        return response;
    }

    public void setResponse(List<CacheRefreshResponse> response) {
        this.response = response;
    }
}

class CacheRefreshResponse {
    private String host;
    private int status;
    private Map<String,String> headers;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
}

class CacheRefresherHealthResponse {
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}