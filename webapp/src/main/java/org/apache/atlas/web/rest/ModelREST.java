package org.apache.atlas.web.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.searchlog.SearchLoggingManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.ModelUtil;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

@Path("model")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ModelREST {


    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.ModelREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);

    @Context
    private HttpServletRequest httpServletRequest;
    private final boolean enableSearchLogging;

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasDiscoveryService discoveryService;
    private final SearchLoggingManagement loggerManagement;

    private static final String INDEXSEARCH_TAG_NAME = "indexsearch";
    private static final Set<String> TRACKING_UTM_TAGS = new HashSet<>(Arrays.asList("ui_main_list", "ui_popup_searchbar"));
    private static final String UTM_TAG_FROM_PRODUCT = "project_webapp";

    @Inject
    public ModelREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService,
                     SearchLoggingManagement loggerManagement) {
        this.typeRegistry = typeRegistry;
        this.discoveryService = discoveryService;
        this.loggerManagement = loggerManagement;
        this.enableSearchLogging = AtlasConfiguration.ENABLE_SEARCH_LOGGER.getBoolean();
    }

    @Path("/search")
    @POST
    @Timed
    public AtlasSearchResult dataSearch(@QueryParam("namespace") String namespace,
                                        @QueryParam("businessDate") String businessDate,
                                        @QueryParam("systemDate") String systemDate,
                                        @Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {

        Servlets.validateQueryParamLength("namespace", namespace);
        Servlets.validateQueryParamLength("businessDate", businessDate);
        AtlasPerfTracer perf = null;
        long startTime = System.currentTimeMillis();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ModelREST.dataSearch(" + parameters + ")");
            }

            parameters = parameters == null ? new IndexSearchParams() : parameters;

            String queryStringUsingFiltersAndUserDSL = ModelUtil.createQueryStringUsingFiltersAndUserDSL(namespace,
                    businessDate,
                    systemDate,
                    parameters.getQuery());

            if (StringUtils.isEmpty(queryStringUsingFiltersAndUserDSL)) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid model search query");
                throw abe;
            }

            parameters.setQuery(queryStringUsingFiltersAndUserDSL);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }

            AtlasSearchResult result = discoveryService.directIndexSearch(parameters);
            if (result == null) {
                return null;
            }


            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog()
            ) {
                // logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog()
            ) {
                //logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } finally {
            if (CollectionUtils.isNotEmpty(parameters.getUtmTags())) {
                AtlasPerfMetrics.Metric indexsearchMetric = new AtlasPerfMetrics.Metric(INDEXSEARCH_TAG_NAME);
                indexsearchMetric.addTag("utmTag", "other");
                indexsearchMetric.addTag("source", "other");
                for (String utmTag : parameters.getUtmTags()) {
                    if (TRACKING_UTM_TAGS.contains(utmTag)) {
                        indexsearchMetric.addTag("utmTag", utmTag);
                        break;
                    }
                }
                if (parameters.getUtmTags().contains(UTM_TAG_FROM_PRODUCT)) {
                    indexsearchMetric.addTag("source", UTM_TAG_FROM_PRODUCT);
                }
                indexsearchMetric.addTag("name", INDEXSEARCH_TAG_NAME);
                indexsearchMetric.setTotalTimeMSecs(System.currentTimeMillis() - startTime);
                RequestContext.get().addApplicationMetrics(indexsearchMetric);
            }
            AtlasPerfTracer.log(perf);
        }
    }

}