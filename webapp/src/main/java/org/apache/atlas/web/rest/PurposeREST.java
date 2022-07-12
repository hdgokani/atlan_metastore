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


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.persona.AtlasPersonaService;
import org.apache.atlas.purpose.AtlasPurposeService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.repository.Constants.*;

/**
 * REST for a Purpose operations
 */
@Path("v2/purpose")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class PurposeREST {

    private static final Logger LOG      = LoggerFactory.getLogger(PurposeREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.PurposeREST");

    private final AtlasPurposeService atlasPurposeService;

    @Inject
    public PurposeREST(AtlasPurposeService atlasPurposeService) {
        this.atlasPurposeService = atlasPurposeService;
    }


    /**
     * Create or Update Purpose
     *
     * @param entityWithExtInfo Purpose entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    public EntityMutationResponse createOrUpdatePurpose(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PurposeREST.createOrUpdate()");
            }

            if(!entityWithExtInfo.getEntity().getTypeName().equals(PURPOSE_ENTITY_TYPE)) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type {}", PURPOSE_ENTITY_TYPE);
            }

            //TODO: Validate tags names?

            response = atlasPurposeService.createOrUpdatePurpose(entityWithExtInfo);
        } finally {
            AtlasPerfTracer.log(perf);
        }

        return response;
    }

    /**
     * Create or Update Purpose policy
     *
     * @param entityWithExtInfo Purpose policy entity
     * @return EntityMutationResponse
     * @throws AtlasBaseException
     */
    @POST
    @Path("policy")
    public EntityMutationResponse createOrUpdatePurposePolicy(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        EntityMutationResponse response;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PurposeREST.createPersonaPolicy()");
            }

            String typeName = entityWithExtInfo.getEntity().getTypeName();
            if(!PURPOSE_METADATA_POLICY_ENTITY_TYPE.equals(typeName)) {
                throw new AtlasBaseException(BAD_REQUEST, "Not a valid type for Purpose Policy");
            }

            response = atlasPurposeService.createOrUpdatePersonaPolicy(entityWithExtInfo);

        } finally {
            AtlasPerfTracer.log(perf);
        }

        return response;
    }
}