/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ranger.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RangerClientCaller {
    private static final Logger LOG = LoggerFactory.getLogger(RangerClientCaller.class);

    protected static WebResource service;

    public static <T> T callAPI(AtlasBaseClient.API api, Class<T> responseType, MultivaluedMap<String, String> queryParams)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, queryParams), null, responseType);
    }

    public static <T> T callAPI(AtlasBaseClient.API api, Class<T> responseType, Object requestObject, String... params)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, params), requestObject, responseType);
    }

    protected static <T> T callAPIWithResource(AtlasBaseClient.API api, WebResource resource, Object requestObject, Class<T> responseType) throws AtlasServiceException {
        GenericType<T> genericType = null;
        if (responseType != null) {
            genericType = new GenericType<>(responseType);
        }
        return callAPIWithResource(api, resource, requestObject, genericType);
    }

    protected static <T> T callAPIWithResource(AtlasBaseClient.API api, WebResource resource, Object requestObject, GenericType<T> responseType) throws AtlasServiceException {
        ClientResponse clientResponse = null;
        int i = 0;
        do {
            if (LOG.isDebugEnabled()) {
                LOG.debug("------------------------------------------------------");
                LOG.debug("Call         : {} {}", api.getMethod(), api.getNormalizedPath());
                LOG.debug("Content-type : {} ", api.getConsumes());
                LOG.debug("Accept       : {} ", api.getProduces());
                if (requestObject != null) {
                    LOG.debug("Request      : {}", requestObject);
                }
            }

            WebResource.Builder requestBuilder = resource.getRequestBuilder();

            // Set content headers
            requestBuilder
                    .accept(api.getProduces())
                    .type(api.getConsumes())
                    .header("Expect", "100-continue");

            clientResponse = requestBuilder.method(api.getMethod(), ClientResponse.class, requestObject);

            LOG.debug("HTTP Status  : {}", clientResponse.getStatus());

            if (!LOG.isDebugEnabled()) {
                LOG.info("method={} path={} contentType={} accept={} status={}", api.getMethod(),
                        api.getNormalizedPath(), api.getConsumes(), api.getProduces(), clientResponse.getStatus());
            }

            if (clientResponse.getStatus() == api.getExpectedStatus().getStatusCode()) {
                if (responseType == null) {
                    return null;
                }
                try {
                    if(api.getProduces().equals(MediaType.APPLICATION_OCTET_STREAM)) {
                        return (T) clientResponse.getEntityInputStream();
                    } else if (responseType.getRawClass().equals(ObjectNode.class)) {
                        String stringEntity = clientResponse.getEntity(String.class);
                        try {
                            JsonNode jsonObject = AtlasJson.parseToV1JsonNode(stringEntity);
                            LOG.debug("Response     : {}", jsonObject);
                            LOG.debug("------------------------------------------------------");
                            return (T) jsonObject;
                        } catch (IOException e) {
                            throw new AtlasServiceException(api, e);
                        }
                    } else {
                        T entity = clientResponse.getEntity(responseType);
                        LOG.debug("Response     : {}", entity);
                        LOG.debug("------------------------------------------------------");
                        return entity;
                    }
                } catch (ClientHandlerException e) {
                    throw new AtlasServiceException(api, e);
                }
            } else if (clientResponse.getStatus() != ClientResponse.Status.SERVICE_UNAVAILABLE.getStatusCode()) {
                break;
            } else {
                LOG.error("Got a service unavailable when calling: {}, will retry..", resource);
                sleepBetweenRetries();
            }

            i++;
        } while (i < getNumberOfRetries());

        throw new AtlasServiceException(api, clientResponse);
    }


    protected static WebResource getResource(AtlasBaseClient.API api, String... pathParams) {
        return getResource(service, api, pathParams);
    }

    // Modify URL to include the query params
    private static WebResource getResource(AtlasBaseClient.API api, MultivaluedMap<String, String> queryParams) {
        WebResource resource = service.path(api.getNormalizedPath());
        resource = appendQueryParams(queryParams, resource);
        return resource;
    }

    // Modify URL to include the path params
    private static WebResource getResource(WebResource service, AtlasBaseClient.API api, String... pathParams) {
        WebResource resource = service.path(api.getNormalizedPath());
        resource = appendPathParams(resource, pathParams);
        return resource;
    }

    private static WebResource appendQueryParams(MultivaluedMap<String, String> queryParams, WebResource resource) {
        if (null != queryParams && !queryParams.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
                for (String value : entry.getValue()) {
                    if (StringUtils.isNotBlank(value)) {
                        resource = resource.queryParam(entry.getKey(), value);
                    }
                }
            }
        }
        return resource;
    }

    private static WebResource appendPathParams(WebResource resource, String[] pathParams) {
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    private static int getNumberOfRetries() {
        //TODO: read from properties
        //return configuration.getInt(AtlasBaseClient.ATLAS_CLIENT_HA_RETRIES_KEY, AtlasBaseClient.DEFAULT_NUM_RETRIES);
        return 3;
    }

    private static int getSleepBetweenRetriesMs() {
        //TODO: read from properties
        //return configuration.getInt(AtlasBaseClient.ATLAS_CLIENT_HA_SLEEP_INTERVAL_MS_KEY, AtlasBaseClient.DEFAULT_SLEEP_BETWEEN_RETRIES_MS);
        return 5000;
    }

    static void sleepBetweenRetries() {
        try {
            Thread.sleep(getSleepBetweenRetriesMs());
        } catch (InterruptedException e) {
            LOG.error("Interrupted from sleeping between retries.", e);
        }
    }
}
