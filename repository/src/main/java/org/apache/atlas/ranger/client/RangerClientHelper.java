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

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.ranger.RangerPolicyList;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Map;

@Component
public class RangerClientHelper {
    private static final Logger LOG = LoggerFactory.getLogger(RangerClientHelper.class);

    private static RangerClient client;

    @Inject
    public RangerClientHelper(RangerClient client) {
        this.client = client;
    }

    public static RangerRole createRole(RangerRole rangerRole) throws AtlasServiceException {
        return client.createRole(rangerRole);
    }

    public static RangerRole updateRole(RangerRole rangerRole) throws AtlasServiceException {
        return client.updateRole(rangerRole);
    }

    public static RangerPolicy createPolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {
        return client.createPolicy(rangerPolicy);
    }

    public static RangerPolicy updatePolicy(RangerPolicy rangerPolicy) throws AtlasServiceException {
        return client.updatePolicy(rangerPolicy);
    }

    public static RangerPolicyList searchPoliciesByResources(Map<String, String> resources,
                                                             Map<String, String> attributes) throws AtlasServiceException {
        RangerPolicyList ret = null;
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("searchPoliciesByResources");

        ret = client.searchPoliciesByResources(resources, attributes);

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    public static RangerPolicyList getPoliciesByLabels(Map<String, String> attributes) throws AtlasServiceException {
        RangerPolicyList ret = null;
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getPoliciesByLabels");

        ret = client.getPoliciesByLabels(attributes);

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    public static void deletePolicy(long policyId) throws AtlasServiceException {
        client.deletePolicyById(policyId);
    }
}
