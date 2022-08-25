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
 package org.apache.atlas.web.rest;
 */

package org.apache.atlas.web.rest;


import org.apache.atlas.accesscontrol.AtlasAccessControlService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicyCategory;
import static org.apache.atlas.repository.Constants.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;

/**
 * REST for a Persona/ Purpose operations
 */
@Path("v2/accesscontrol")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class AccessControlREST {

    private static final Logger LOG      = LoggerFactory.getLogger(AccessControlREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AccessControlREST");


    private final AtlasAccessControlService accessControlService;

    @Inject
    public AccessControlREST(AtlasAccessControlService accessControlService) {
        this.accessControlService = accessControlService;
    }

    /**
     * Create or Update Persona/Purpose
     *
     * @param entityWithExtInfo Persona/Purpose entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    public EntityMutationResponse createOrUpdate(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException, JSONException, IOException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AccessControlREST.createOrUpdate()");
            }

            return accessControlService.createOrUpdate(entityWithExtInfo);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Delete a Persona/Purpose
     *
     * @param guid of Persona/purpose entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @DELETE
    @Path("/{guid}")
    public void delete(@PathParam("guid") String guid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AccessControlREST.delete()");
            }

            accessControlService.delete(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Create or Update an access control policy
     *
     * @param entityWithExtInfo access control policy entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    @Path("policy")
    public EntityMutationResponse createOrUpdatePolicy(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AccessControlREST.createOrUpdateAccessControlPolicy()");
            }

            if(!POLICY_ENTITY_TYPE.equals(entityWithExtInfo.getEntity().getTypeName())) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type {}", POLICY_ENTITY_TYPE);
            }

            if(StringUtils.isEmpty(getPolicyCategory(entityWithExtInfo.getEntity()))) {
                throw new AtlasBaseException(BAD_REQUEST, "Policy must have accessControlPolicyCategory {}", POLICY_CATEGORY_PERSONA);
            }

            response = accessControlService.createOrUpdatePolicy(entityWithExtInfo);

        } finally {
            AtlasPerfTracer.log(perf);
        }

        return response;
    }

    /**
     * Delete an access control Policy
     *
     * @param guid of access control Policy entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @DELETE
    @Path("policy/{guid}")
    public void deletePolicy(@PathParam("guid") String guid) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "AccessControlREST.deleteAccessControlPolicy()");
            }

            accessControlService.deletePolicy(guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}