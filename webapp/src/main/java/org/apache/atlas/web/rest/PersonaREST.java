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
import org.apache.atlas.repository.store.AtlasPersonaService;
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
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;

/**
 * REST for a Persona operations
 */
@Path("v2/persona")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class PersonaREST {

    private static final Logger LOG      = LoggerFactory.getLogger(PersonaREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.PersonaREST");


    private final AtlasPersonaService atlasPersonaService;

    @Inject
    public PersonaREST(AtlasPersonaService atlasPersonaService) {
        this.atlasPersonaService = atlasPersonaService;
    }

    @POST
    public EntityMutationResponse createPersona(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PersonaREST.createOrUpdate()");
            }

            if(!entityWithExtInfo.getEntity().getTypeName().equals(PERSONA_ENTITY_TYPE)) {
                throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type {}", PERSONA_ENTITY_TYPE);
            }

            return atlasPersonaService.createRole(entityWithExtInfo);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }
}