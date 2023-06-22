package org.apache.atlas.service.metrics;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;

import static java.util.Optional.ofNullable;

@Component
@ConditionalOnAtlasProperty(property = "atlas.prometheus.service.impl")
public class MetricsRegistryServiceImpl implements MetricsRegistry {

    public static final String SERVICE = "service";
    public static final String ATLAS_METASTORE = "atlas-metastore";
    public static final String TRACE_ID = "trace_id";
    public static final String COUNT = "count";
    public static final String TIME_TAKEN = "timeTaken";
    public static final String NAME = "name";
    public static final String METHOD_REQUEST = "method_request";

    private final PrometheusMeterRegistry prometheusMeterRegistry;

    @Inject
    public MetricsRegistryServiceImpl(PrometheusMeterRegistry prometheusMeterRegistry, ApplicationContext applicationContext) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.prometheusMeterRegistry.config().withHighCardinalityTagsDetector().commonTags(SERVICE, ATLAS_METASTORE);
    }

    @Override
    public void collect(String requestId, AtlasPerfMetrics metrics) {
        for (String name : metrics.getMetricsNames()) {
            AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
            this.prometheusMeterRegistry.counter(METHOD_REQUEST, NAME, metric.getName(), TRACE_ID, ofNullable(requestId).orElse("-1"), COUNT, String.valueOf(metric.getInvocations()), TIME_TAKEN, String.valueOf(metric.getTotalTimeMSecs())).increment();
        }
    }

    @Override
    public void scrape(PrintWriter writer) throws IOException {
        try {
            this.prometheusMeterRegistry.scrape(writer);
            this.prometheusMeterRegistry.clear();
            writer.flush();
        } finally {
            writer.close();
        }
    }

}
