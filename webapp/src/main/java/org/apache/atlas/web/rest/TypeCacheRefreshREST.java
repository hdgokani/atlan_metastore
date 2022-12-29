package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.IAtlasGraphProvider;
import org.apache.atlas.repository.graph.TypeCacheRefresher.RefreshOperation;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.service.ActiveIndexNameManager;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.web.service.AtlasHealthStatus;
import org.apache.atlas.web.service.ElasticInstanceConfigService;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.service.TypeSyncService;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.FAILED_TO_REFRESH_TYPE_DEF_CACHE;
import static org.apache.atlas.service.ActiveIndexNameManager.getCurrentReadVertexIndexName;


@Path("admin/types")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TypeCacheRefreshREST {
    private static final Logger LOG = LoggerFactory.getLogger(TypeCacheRefreshREST.class);

    private final AtlasTypeDefStore typeDefStore;
    private final IAtlasGraphProvider provider;
    private final ServiceState serviceState;
    private final AtlasHealthStatus atlasHealthStatus;
    private final TypeSyncService typeSyncService;
    private final ElasticInstanceConfigService elasticInstanceConfigService;

    @Inject
    public TypeCacheRefreshREST(AtlasTypeDefStore typeDefStore, IAtlasGraphProvider provider,
                                ServiceState serviceState, AtlasHealthStatus atlasHealthStatus,
                                TypeSyncService typeSyncService,
                                ElasticInstanceConfigService elasticInstanceConfigService) {
        this.typeDefStore = typeDefStore;
        this.provider = provider;
        this.serviceState = serviceState;
        this.atlasHealthStatus = atlasHealthStatus;
        this.typeSyncService = typeSyncService;
        this.elasticInstanceConfigService = elasticInstanceConfigService;
    }

    /**
     * API to refresh type-def cache.
     *
     * @throws AtlasBaseException
     * @HTTP 204 if type def cache is refreshed successfully
     * @HTTP 500 if there is an error refreshing type def cache
     */
    @POST
    @Path("/refresh")
    @Timed
    public void refreshCache(@QueryParam("operationId") @DefaultValue("0") int operationId,
                             @QueryParam("expectedFieldKeys") int expectedFieldKeys,
                             @QueryParam("traceId") String traceId) throws AtlasBaseException {
        try {
            if (serviceState.getState() != ServiceState.ServiceStateValue.ACTIVE) {
                LOG.warn("Node is in {} state. skipping refreshing type-def-cache :: traceId {}, operationId {}",
                        serviceState.getState(), traceId, operationId);
                return;
            }

            if (operationId == RefreshOperation.ONLY_TYPE.getId()) {
                refreshTypeDef(expectedFieldKeys, traceId);
            } else if (operationId == RefreshOperation.TYPE_WRITE_INDEX.getId()) {
                refreshTypeDef(expectedFieldKeys, traceId);
                refreshWriteIndexName(traceId);
            } else if (operationId == RefreshOperation.READ_INDEX.getId()) {
                refreshReadIndexName(traceId);
            } else if (operationId == RefreshOperation.WAIT_COMPLETE_REQUESTS.getId()) {
                typeSyncService.waitAllRequestsToComplete(traceId);
            }

        } catch (Exception e) {
            LOG.error("Error during refreshing cache  :: traceId " + traceId + " " + e.getMessage(), e);
            serviceState.setState(ServiceState.ServiceStateValue.PASSIVE, true);
            atlasHealthStatus.markUnhealthy(AtlasHealthStatus.Component.TYPE_DEF_CACHE, "type-def-cache is not in sync");
            throw new AtlasBaseException(FAILED_TO_REFRESH_TYPE_DEF_CACHE);
        }
    }

    private void refreshTypeDef(int expectedFieldKeys,final String traceId) throws RepositoryException, InterruptedException, AtlasBaseException {
        LOG.info("Initiating type-def cache refresh with expectedFieldKeys = {} :: traceId {}", expectedFieldKeys,traceId);
        AtlasGraphManagement management = provider.get().getManagementSystem();

        try {
            int currentSize = management.getGraphIndex(getCurrentReadVertexIndexName()).getFieldKeys().size();
            LOG.info("Size of field keys before refresh = {} :: traceId {}", currentSize,traceId);

            long totalWaitTimeInMillis = 15 * 1000;//15 seconds
            long sleepTimeInMillis = 500;
            long totalIterationsAllowed = Math.floorDiv(totalWaitTimeInMillis, sleepTimeInMillis);
            int counter = 0;

            while (currentSize != expectedFieldKeys && counter++ < totalIterationsAllowed) {
                currentSize = management.getGraphIndex(getCurrentReadVertexIndexName()).getFieldKeys().size();
                LOG.info("field keys size found = {} at iteration {} :: traceId {}", currentSize, counter, traceId);
                Thread.sleep(sleepTimeInMillis);
            }
            //This condition will hold true when expected fieldKeys did not appear even after waiting for totalWaitTimeInMillis
            if (counter > totalIterationsAllowed) {
                final String errorMessage = String.format("Could not find desired count of fieldKeys %d after %d ms of wait. Current size of field keys is %d :: traceId %s",
                        expectedFieldKeys, totalWaitTimeInMillis, currentSize, traceId);
                throw new AtlasBaseException(errorMessage);
            } else {
                LOG.info("Found desired size of fieldKeys in iteration {} :: traceId {}", counter, traceId);
            }
            //Reload in-memory cache of type-registry
            typeDefStore.init();

            LOG.info("Size of field keys after refresh = {}", management.getGraphIndex(getCurrentReadVertexIndexName()).getFieldKeys().size());
            LOG.info("Completed type-def cache refresh :: traceId {}", traceId);

        } finally {
            management.commit();
        }
    }

    private void refreshWriteIndexName(final String traceId) {
        LOG.info("Refreshing write index name of ES :: traceId {}", traceId);

        String newIndexName = elasticInstanceConfigService.getCurrentIndexName();
        ActiveIndexNameManager.setCurrentWriteVertexIndexName(newIndexName);

        LOG.info("Refreshed write index name of ES :: traceId {}, newWriteIndexName {}", traceId, newIndexName);
    }

    private void refreshReadIndexName(final String traceId) {
        LOG.info("Refreshing read index name of ES :: traceId {}", traceId);
        RequestContext.setIsTypeSyncMode(false);

        String newIndexName = elasticInstanceConfigService.getCurrentIndexName();
        ActiveIndexNameManager.setCurrentReadVertexIndexName(newIndexName);

        LOG.info("Refreshed read index name of ES :: traceId {}, newReadIndexName {}", traceId, newIndexName);
    }
}
