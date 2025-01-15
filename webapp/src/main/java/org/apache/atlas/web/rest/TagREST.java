package org.apache.atlas.web.rest;

import org.apache.atlas.web.util.Servlets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("tag")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class TagREST {
    private static final Logger LOG = LoggerFactory.getLogger(TagREST.class);

    @Inject
    public TagREST() {
    }

    @GET
    @Path("/test")
    public int test() {
        LOG.info("this is a test endpoint!");
        return 0;
    }
}
