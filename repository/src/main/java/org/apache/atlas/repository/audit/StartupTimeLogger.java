package org.apache.atlas.repository.audit;

import org.apache.atlas.utils.AtlasPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class StartupTimeLogger implements ApplicationListener<ContextRefreshedEvent> {
    private final StartupTimeLoggerBeanPostProcessor beanPostProcessor;

    private static final Logger LOG = LoggerFactory.getLogger(StartupTimeLogger.class);

    public StartupTimeLogger(StartupTimeLoggerBeanPostProcessor beanPostProcessor) {
        this.beanPostProcessor = beanPostProcessor;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Print the startup times after all beans are loaded

        LOG.info("Capturing Bean creation time");
      printHashMapInTableFormatDescendingOrder(beanPostProcessor.getDurationTimeMap(), "creationTime");
    }

    public static void printHashMapInTableFormatDescendingOrder(Map<String, Long> map, String value) {
        // Convert map to a list of entries
        List<Map.Entry<String, Long>> list = new ArrayList<>(map.entrySet());

        // Sort the list by values in descending order
        list.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

        // Find the longest key to determine column width
        int maxKeyLength = list.stream().map(entry -> entry.getKey().length()).max(Integer::compare).orElse(0);

        // Create format string for printing each row
        String rowFormat = "| %-" + maxKeyLength + "s | %-1s |\n";

        // Print table header
        System.out.printf(rowFormat, "Key", value);
        System.out.println(new String(new char[maxKeyLength + 1]).replace('\0', '-'));

        // Print each sorted entry
        for (Map.Entry<String, Long> entry : list) {
            System.out.printf(rowFormat, entry.getKey(), entry.getValue());
        }
    }
}