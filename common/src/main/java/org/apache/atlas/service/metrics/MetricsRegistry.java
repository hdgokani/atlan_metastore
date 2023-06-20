package org.apache.atlas.service.metrics;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Optional;

@Component
public class MetricsRegistry {

    public static final String SERVICE = "service";
    public static final String ATLAS_METASTORE = "atlas-metastore";
    public static final String TRACE_ID = "trace_id";
    public static final String COUNT = "count";
    public static final String TIME_TAKEN = "timeTaken";

    private final PrometheusMeterRegistry prometheusMeterRegistry;

    @Autowired
    public MetricsRegistry(PrometheusMeterRegistry prometheusMeterRegistry) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.prometheusMeterRegistry.config().withHighCardinalityTagsDetector().commonTags(SERVICE, ATLAS_METASTORE);
    }

    public void collect(String requestId, AtlasPerfMetrics metrics) {
        for (String name : metrics.getMetricsNames()) {
            AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
            this.prometheusMeterRegistry.counter(metric.getName(), TRACE_ID, requestId, COUNT, String.valueOf(metric.getInvocations()), TIME_TAKEN, String.valueOf(metric.getTotalTimeMSecs())).increment();
        }
    }

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
