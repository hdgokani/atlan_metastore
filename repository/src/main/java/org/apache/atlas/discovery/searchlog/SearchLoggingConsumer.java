package org.apache.atlas.discovery.searchlog;

import org.apache.atlas.model.discovery.searchlog.SearchRequestLogData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchLoggingConsumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SearchLoggingConsumer.class);

    private final List<SearchLogger> esSearchLoggers;
    private final SearchRequestLogData searchRequestLogData;

    public SearchLoggingConsumer(List<SearchLogger> esSearchLoggers, SearchRequestLogData searchRequestLogData) {
        this.esSearchLoggers = esSearchLoggers;
        this.searchRequestLogData = searchRequestLogData;
    }

    @Override
    public void run() {
        LOG.info("SearchLoggerConsumer: run() {}", Thread.currentThread().getId());
        for (SearchLogger esSearchLogger : esSearchLoggers) {
            esSearchLogger.log(searchRequestLogData);
        }
    }
}