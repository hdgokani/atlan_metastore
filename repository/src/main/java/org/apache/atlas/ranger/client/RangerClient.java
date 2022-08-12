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

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.multipart.impl.MultiPartWriter;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.ranger.RangerPolicyList;
import org.apache.atlas.ranger.RangerRoleList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.HashMap;
import java.util.Map;

import static javax.ws.rs.HttpMethod.DELETE;
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.HttpMethod.PUT;
import static org.apache.atlas.accesscontrol.persona.AtlasPersonaUtil.RESOURCE_PREFIX;

@Component
public class RangerClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerClient.class);


    protected Configuration configuration;
    private String basicAuthUser;
    private String basicAuthPassword;


    private static final String PROP_RANGER_BASE_URL = "atlas.ranger.base.url";
    private static final String PROP_RANGER_USERNAME = "atlas.ranger.username";
    private static final String PROP_RANGER_PASSWORD = "atlas.ranger.password";

    private static final String BASE_URI_DEFAULT = "http://localhost:8080/api/policy/";


    private static String BASE_URL;

    public static final  String POLICY_GET_BY_NAME = "public/v2/api/service/%s/policy/%s";
    public static final  String POLICY_GET_BY_ID = "service/plugins/policies/%s";
    public static final  String POLICY_DELETE_BY_ID = "service/plugins/policies/%s";
    public static final  String CREATE_ROLE = "service/roles/roles";
    public static final  String GET_ROLE_BY_NAME = "service/roles/lookup/roles";
    public static final  String UPDATE_ROLE = "service/roles/roles/%s";
    public static final  String DELETE_ROLE = "service/roles/roles/%s";
    public static final  String CREATE_POLICY = "service/public/v2/api/policy";
    //public static final  String CREATE_POLICY = "service/plugins/policies";
    public static final  String UPDATE_POLICY = "service/public/v2/api/policy/%s";
    //public static final  String UPDATE_POLICY = "service/plugins/policies/%s";
    public static final  String SEARCH_BY_RESOURCES = "service/plugins/policies";
    public static final  String SEARCH_BY_LABELS = "service/plugins/policies";


    public RangerClient() throws AtlasException {

        BASE_URL = ApplicationProperties.get().getString(PROP_RANGER_BASE_URL, BASE_URI_DEFAULT);

        this.basicAuthUser = ApplicationProperties.get().getString(PROP_RANGER_USERNAME, "admin");
        this.basicAuthPassword = ApplicationProperties.get().getString(PROP_RANGER_PASSWORD);

        String[] basicAuthUserNamePassword = new String[2];
        basicAuthUserNamePassword[0] = basicAuthUser;
        basicAuthUserNamePassword[1] = basicAuthPassword;

        try {
            init();
        } catch (AtlasException e) {
            e.printStackTrace();
        }
    }

    private void init() throws AtlasException {
        this.configuration = getClientProperties();
        Client client = getClient(configuration);

        if (StringUtils.isNotEmpty(basicAuthUser) && StringUtils.isNotEmpty(basicAuthPassword)) {
            final HTTPBasicAuthFilter authFilter = new HTTPBasicAuthFilter(basicAuthUser, basicAuthPassword);
            client.addFilter(authFilter);

        }

        RangerClientCaller.service = client.resource(UriBuilder.fromUri(BASE_URL).build());
    }

    protected Client getClient(Configuration configuration) {
        DefaultClientConfig config = new DefaultClientConfig();
        // Enable POJO mapping feature
        config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        config.getClasses().add(JacksonJaxbJsonProvider.class);
        config.getClasses().add(MultiPartWriter.class);

        int readTimeout = configuration.getInt("atlas.ranger.client.readTimeoutMSecs", 60000);
        int connectTimeout = configuration.getInt("atlas.ranger.client.connectTimeoutMSecs", 60000);

        final URLConnectionClientHandler handler = new URLConnectionClientHandler();

        Client client = new Client(handler, config);
        client.setReadTimeout(readTimeout);
        client.setConnectTimeout(connectTimeout);
        return client;
    }

    protected Configuration getClientProperties() throws AtlasException {
        return ApplicationProperties.get();
    }

    public RangerPolicy getPolicyById(String policyId) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_GET_BY_ID, policyId), GET, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerPolicy.class, null);
    }


    public String getPolicyByServicePolicyName(String serviceName, String policyName) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_GET_BY_NAME, serviceName, policyName), GET, Response.Status.OK);

        return RangerClientCaller.callAPI(api, String.class, null);
    }

    public RangerRole createRole(RangerRole rangerRole) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(CREATE_ROLE, POST, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerRole.class, rangerRole);
    }

    public RangerRoleList getRole(String roleName) throws AtlasServiceException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("roleNamePartial", roleName);

        MultivaluedMap<String, String> queryParams = toQueryParams(attrs, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(GET_ROLE_BY_NAME, GET, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerRoleList.class, queryParams);
    }

    public RangerRole updateRole(RangerRole rangerRole) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(UPDATE_ROLE, rangerRole.getId()), PUT, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerRole.class, rangerRole);
    }

    public void deleteRole(long roleId) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(DELETE_ROLE, roleId), DELETE, Response.Status.NO_CONTENT);

        RangerClientCaller.callAPI(api, (Class<?>)null, null);
    }

    public RangerPolicy createPolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(CREATE_POLICY, POST, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerPolicy.class, rangerPolicy);
    }

    public RangerPolicy updatePolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {

        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(UPDATE_POLICY, rangerPolicy.getId()), PUT, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerPolicy.class, rangerPolicy);
    }

    public RangerPolicyList searchPoliciesByResources(Map<String, String> resources, Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = resourcesToQueryParams(resources);
        queryParams = toQueryParams(attributes, queryParams);

        AtlasBaseClient.API api = new AtlasBaseClient.API(SEARCH_BY_RESOURCES, GET, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerPolicyList.class, queryParams);
    }

    public RangerPolicyList getPoliciesByLabels(Map<String, String> attributes) throws AtlasServiceException {
        MultivaluedMap<String, String> queryParams = toQueryParams(attributes, null);

        AtlasBaseClient.API api = new AtlasBaseClient.API(SEARCH_BY_LABELS, GET, Response.Status.OK);

        return RangerClientCaller.callAPI(api, RangerPolicyList.class, queryParams);
    }

    public void deletePolicyById(Long policyId) throws AtlasServiceException {
        AtlasBaseClient.API api = new AtlasBaseClient.API(String.format(POLICY_DELETE_BY_ID, policyId), DELETE, Response.Status.NO_CONTENT);

        RangerClientCaller.callAPI(api, (Class<?>)null, null);
    }

    private MultivaluedMap<String, String> resourcesToQueryParams(Map<String, String> attributes) {
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();

        if (MapUtils.isNotEmpty(attributes)) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                queryParams.putSingle(RESOURCE_PREFIX + e.getKey(), e.getValue());
            }
        }

        return queryParams;
    }

    private MultivaluedMap<String, String> toQueryParams(Map<String, String> attributes,
                                                         MultivaluedMap<String, String> queryParams) {
        if (queryParams == null) {
            queryParams = new MultivaluedMapImpl();
        }

        if (MapUtils.isNotEmpty(attributes)) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                queryParams.putSingle(e.getKey(), e.getValue());
            }
        }

        return queryParams;
    }
}

