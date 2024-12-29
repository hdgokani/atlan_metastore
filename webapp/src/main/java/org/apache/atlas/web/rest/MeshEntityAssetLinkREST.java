package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.LinkMeshEntityRequest;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import static org.apache.atlas.repository.util.AccessControlUtils.ARGO_SERVICE_USER_NAME;

@Path("mesh-asset-link")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class MeshEntityAssetLinkREST {

    private static final Logger LOG = LoggerFactory.getLogger(MeshEntityAssetLinkREST.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ProductAssetLinkREST");

    private final AtlasEntityStore entitiesStore;

    @Inject
    public MeshEntityAssetLinkREST(AtlasEntityStore entitiesStore) {
        this.entitiesStore = entitiesStore;
    }

    /**
     * Links a product to entities.
     *
     * @param request    the request containing the GUIDs of the assets to link domain to
     * @throws AtlasBaseException if there is an error during the linking process
     */

    @POST
    @Path("/link-domain")
    @Timed
    public void linkDomainToAssets(final LinkMeshEntityRequest request) throws AtlasBaseException {
        String domainGuid = request.getDomainGuid();
        if(domainGuid == null || domainGuid.isEmpty()) {
            throw new AtlasBaseException("Domain GUID is required for linking domain to assets");
        }

        LOG.info("Linking Domain {} to Asset", domainGuid);

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(true);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "domain-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.linkDomainToAssets(" + domainGuid + ")");
            }

            // Link the domain to the specified entities
            entitiesStore.linkMeshEntityToAssets(domainGuid, request.getAssetGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Unlinks a product from entities.
     *
     * @param request    the request containing the GUIDs of the assets to unlink the domain from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/unlink-domain")
    @Timed
    public void unlinkDomainFromAssets(final LinkMeshEntityRequest request) throws AtlasBaseException {
        String domainGuid = request.getDomainGuid();

        LOG.info("Unlinking Domain {} to Asset", domainGuid);

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(true);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "domain-asset-unlink");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.unlinkDomainFromAssets(" + domainGuid + ")");
            }

            // Unlink the domain from the specified entities
            entitiesStore.unlinkMeshEntityFromAssets(domainGuid, request.getAssetGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Links a product to entities.
     *
     * @param request    the request containing the GUIDs of the assets to link product to
     * @throws AtlasBaseException if there is an error during the linking process
     */

    @POST
    @Path("/link-product")
    @Timed
    public void linkProductToAssets(final LinkMeshEntityRequest request) throws AtlasBaseException {
        String productGuid = request.getProductGuid();
        if(productGuid == null || productGuid.isEmpty()) {
            throw new AtlasBaseException("Product GUID is required for linking product to assets");
        }

        // Ensure the current user is authorized to move the policy
        String currentUser = RequestContext.getCurrentUser();
        if (!ARGO_SERVICE_USER_NAME.equals(currentUser)) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, currentUser, "moveBusinessPolicy");
        }

        LOG.info("Linking Product {} to Asset", productGuid);

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(true);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "product-asset-link");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.linkProductToAssets(" + productGuid + ")");
            }

            // Link the product to the specified entities
            entitiesStore.linkMeshEntityToAssets(productGuid, request.getAssetGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Unlinks a product from entities.
     *
     * @param request    the request containing the GUIDs of the assets to unlink the product from
     * @throws AtlasBaseException if there is an error during the unlinking process
     */
    @POST
    @Path("/unlink-domain")
    @Timed
    public void unlinkProductFromAssets(final LinkMeshEntityRequest request) throws AtlasBaseException {
        String productGuid = request.getDomainGuid();

        LOG.info("Unlinking Product {} from Asset", productGuid);

        // Ensure the current user is authorized to move the policy
        String currentUser = RequestContext.getCurrentUser();
        if (!ARGO_SERVICE_USER_NAME.equals(currentUser)) {
            throw new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, currentUser, "moveBusinessPolicy");
        }

        // Set request context parameters
        RequestContext.get().setIncludeClassifications(true);
        RequestContext.get().setIncludeMeanings(false);
        RequestContext.get().getRequestContextHeaders().put("route", "product-asset-unlink");

        AtlasPerfTracer perf = null;
        try {
            // Start performance tracing if enabled
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "MeshEntityAssetLinkREST.unlinkProductFromAssets(" + productGuid + ")");
            }

            // Unlink the domain from the specified entities
            entitiesStore.unlinkMeshEntityFromAssets(productGuid, request.getAssetGuids());
        } finally {
            // Log performance metrics
            AtlasPerfTracer.log(perf);
        }
    }
}