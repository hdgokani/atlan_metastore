/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.tasks;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.poi.ss.formula.functions.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class TaskQueueWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcher.class);
    private static final TaskExecutor.TaskLogger TASK_LOG         = TaskExecutor.TaskLogger.getLogger();

    private TaskRegistry registry;
    private final ExecutorService executorService;
    private final Map<String, TaskFactory> taskTypeFactoryMap;
    private final TaskManagement.Statistics statistics;

    private CountDownLatch latch = null;

    public TaskQueueWatcher(ExecutorService executorService, TaskRegistry registry,
                            Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
        this.registry = registry;
        this.executorService = executorService;
        this.taskTypeFactoryMap = taskTypeFactoryMap;
        this.statistics = statistics;
    }

    @Override
    public void run() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
        }

        try {
            if (latch != null && latch.getCount() != 0) {
                LOG.info("TaskQueueWatcher: Waiting on Latch, current count: {}", latch.getCount());
                latch.await();
            }

            if (latch != null) {
                LOG.info("TaskQueueWatcher: Latch wait complete!!");
            }

            Thread tasksFetcherThread = new Thread(new TasksFetcher(registry, latch, executorService, taskTypeFactoryMap, statistics));
            tasksFetcherThread.start();

        } catch (InterruptedException e) {
            LOG.error("Await on latch interrupted");
        }
    }

    static class TasksFetcher implements Runnable {
        private CountDownLatch latch;
        private TaskRegistry registry;
        private ExecutorService executorService;
        private final TaskManagement.Statistics statistics;
        private final Map<String, TaskFactory> taskTypeFactoryMap;

        public TasksFetcher(TaskRegistry registry, CountDownLatch latch, ExecutorService executorService,
                                Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
            this.latch = latch;
            this.registry = registry;
            this.statistics = statistics;
            this.executorService = executorService;
            this.taskTypeFactoryMap = taskTypeFactoryMap;
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()){
                LOG.debug("TasksFetcher: Fetching tasks for queuing");
            }

            LOG.info("TasksFetcher: fetching tasks for queuing");
            addAll(registry.getTasksForReQueue());
        }

        private void addAll(List<AtlasTask> tasks) {
            if (CollectionUtils.isNotEmpty(tasks)) {
                latch = new CountDownLatch(tasks.size());

                for (AtlasTask task : tasks) {
                    if (task == null) {
                        continue;
                    }
                    TASK_LOG.log(task);

                    this.executorService.submit(new TaskExecutor.TaskConsumer(task, this.registry, this.taskTypeFactoryMap, this.statistics, latch));
                }

                LOG.info("TasksFetcher: Submitted {} tasks to the queue", tasks.size());
            } else {
                if (LOG.isDebugEnabled()){
                    LOG.debug("TasksFetcher: No task to queue");
                }
                LOG.info("TasksFetcher: No task to queue");
            }
        }
    }
}
