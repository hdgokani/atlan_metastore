package org.apache.atlas.web.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class GzipCompressionFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GzipCompressionFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("GzipCompressionFilter initialized");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        HttpServletRequest httpRequest = (HttpServletRequest) request;

        // Check if the client accepts gzip encoding
        String acceptEncoding = httpRequest.getHeader("Accept-Encoding");
        if (acceptEncoding != null && acceptEncoding.contains("gzip") && httpResponse.getHeader("Content-Encoding") == null) {
            GzipResponseWrapper gzipResponseWrapper = new GzipResponseWrapper(httpResponse);
            chain.doFilter(request, gzipResponseWrapper);
            gzipResponseWrapper.close();
        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
        LOG.info("GzipCompressionFilter destroyed");
    }
}