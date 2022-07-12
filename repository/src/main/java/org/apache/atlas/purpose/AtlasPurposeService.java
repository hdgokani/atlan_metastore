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
package org.apache.atlas.purpose;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.ranger.AtlasRangerService;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.purpose.AtlasPurposeUtil.*;
import static org.apache.atlas.repository.Constants.*;


@Component
public class AtlasPurposeService {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPurposeService.class);

    private AtlasRangerService atlasRangerService;
    private final AtlasEntityStore entityStore;
    private final AtlasGraph graph;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;
    private final EntityDiscoveryService entityDiscoveryService;

    @Inject
    public AtlasPurposeService(AtlasRangerService atlasRangerService,
                               AtlasEntityStore entityStore,
                               AtlasTypeRegistry typeRegistry,
                               EntityDiscoveryService entityDiscoveryService,
                               EntityGraphRetriever entityRetriever,
                               AtlasGraph graph) {
        this.entityStore = entityStore;
        this.atlasRangerService = atlasRangerService;
        this.typeRegistry = typeRegistry;
        this.graph = graph;
        this.entityDiscoveryService = entityDiscoveryService;
        //this.entityRetriever = new EntityGraphRetriever(graph, typeRegistry);
        this.entityRetriever = entityRetriever;
    }

    public EntityMutationResponse createOrUpdatePurpose(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        EntityMutationResponse ret = null;
        try {
            AtlasEntity purpose = entityWithExtInfo.getEntity();
            AtlasEntity existingPurposeEntity = null;

            PurposeContext context = new PurposeContext(entityWithExtInfo);

            try {
                Map<String, Object> uniqueAttributes = mapOf(QUALIFIED_NAME, getQualifiedName(purpose));
                existingPurposeEntity = entityRetriever.toAtlasEntity(new AtlasObjectId(purpose.getGuid(), purpose.getTypeName(), uniqueAttributes));
            } catch (AtlasBaseException abe) {
                if (abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_GUID_NOT_FOUND &&
                        abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                    throw abe;
                }
            }

            if (existingPurposeEntity == null) {
                ret = createPurpose(context, entityWithExtInfo);
            } else {
                ret = updatePurpose(context, existingPurposeEntity);
            }

        } catch (AtlasBaseException abe) {
            //TODO: handle exception
            abe.printStackTrace();
            LOG.error("Failed to create perpose");
            throw abe;
        }
        return ret;
    }

    private EntityMutationResponse createPurpose(PurposeContext context, AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        EntityMutationResponse ret = null;

        return ret;
    }

    private EntityMutationResponse updatePurpose(PurposeContext context, AtlasEntity existingPurposeEntity) {
        EntityMutationResponse ret = null;

        return ret;
    }

    public EntityMutationResponse createOrUpdatePersonaPolicy(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException  {
        EntityMutationResponse ret = null;

        return ret;
    }
}
