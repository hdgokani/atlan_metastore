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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TaskExecutor {
    private static final Logger     PERF_LOG         = AtlasPerfTracer.getPerfLogger("atlas.task");
    private static final Logger     LOG              = LoggerFactory.getLogger(TaskExecutor.class);
    private static final TaskLogger TASK_LOG         = TaskLogger.getLogger();
    private static final String     TASK_NAME_FORMAT = "atlas-task-%d-";

    private static final boolean perfEnabled = AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG);

    private final ScheduledExecutorService  watcherExecutor;
    private final ExecutorService           taskExecutorService;
    private final TaskQueueWatcher          watcher;

    public TaskExecutor(TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics) {
        this.taskExecutorService    = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                                    .setDaemon(true)
                                                                    .setNameFormat(TASK_NAME_FORMAT + Thread.currentThread().getName())
                                                                    .build());
        this.watcherExecutor    = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                                    .setDaemon(true)
                                                                    .setNameFormat("TaskQueueWatcher")
                                                                    .build());
        watcher = new TaskQueueWatcher(taskExecutorService, registry, taskTypeFactoryMap, statistics);
    }

    public void addAll() {
        long delay = AtlasConfiguration.TASKS_REQUEUE_POLL_INTERVAL.getLong();
        watcherExecutor.scheduleWithFixedDelay(watcher, 0, delay, TimeUnit.MILLISECONDS);
    }

    /*
    * This method called from TaskConsumer
    * If latch count is 0 run TaskQueueWatcher once to reload nex set of tasks if any
    * If no tasks found, this TaskQueueWatcher thread will get killed
    * Still TaskQueueWatcher scheduled with scheduleWithFixedDelay will be alive
    * */
    public void reloadQueueIfEmpty(CountDownLatch latch) {
        LOG.info("Re fill queue as all tasks are processed");
        if (latch != null && latch.getCount() == 0) {
            new Thread(watcher).start();
        }
    }

    //only called from Unit tests
    public void addAll(List<AtlasTask> tasks) {

    }

    @VisibleForTesting
    void waitUntilDone() throws InterruptedException {
        Thread.sleep(5000);
    }

    static class TaskConsumer implements Runnable {
        private static final int MAX_ATTEMPT_COUNT = 3;

        private final Map<String, TaskFactory>  taskTypeFactoryMap;
        private final TaskRegistry              registry;
        private final TaskManagement.Statistics statistics;
        private final AtlasTask                 task;
        private CountDownLatch  latch;

        AtlasPerfTracer perf = null;

        public TaskConsumer(AtlasTask task, TaskRegistry registry, Map<String, TaskFactory> taskTypeFactoryMap, TaskManagement.Statistics statistics,
                            CountDownLatch latch) {
            this.task               = task;
            this.registry           = registry;
            this.taskTypeFactoryMap = taskTypeFactoryMap;
            this.statistics         = statistics;
            this.latch = latch;
        }

        @Override
        public void run() {
            AtlasVertex taskVertex = null;
            int         attemptCount;

            try {
                if (task == null) {
                    TASK_LOG.info("Task not scheduled as it was not found");
                    return;
                }

                taskVertex = registry.getVertex(task.getGuid());
                if (taskVertex == null) {
                    TASK_LOG.warn("Task not scheduled as vertex not found", task);
                }

                if (task.getStatus() == AtlasTask.Status.COMPLETE) {
                    TASK_LOG.warn("Task not scheduled as status was COMPLETE!", task);
                }

                if (perfEnabled) {
                    perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, String.format("atlas.task:%s", task.getGuid(), task.getType()));
                }

                statistics.increment(1);

                attemptCount = task.getAttemptCount();

                if (attemptCount >= MAX_ATTEMPT_COUNT) {
                    TASK_LOG.warn("Max retry count for task exceeded! Skipping!", task);

                    return;
                }

                performTask(taskVertex, task);

            } catch (InterruptedException exception) {
                registry.updateStatus(taskVertex, task);
                TASK_LOG.error("{}: {}: Interrupted!", task, exception);

                statistics.error();
            } catch (Exception exception) {
                if (task != null) {
                    registry.updateStatus(taskVertex, task);

                    TASK_LOG.error("Error executing task. Please perform the operation again!", task, exception);
                } else {
                    LOG.error("Error executing. Please perform the operation again!", exception);
                }

                statistics.error();
            } finally {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOG.info("END task {}: {}", task.getGuid(), task.getType());
                if (task != null) {
                    this.registry.commit();

                    TASK_LOG.log(task);
                }

                latch.countDown();
                //TODO: if queue is empty, reload
                RequestContext.get().clearCache();
                AtlasPerfTracer.log(perf);
            }
        }

        private void performTask(AtlasVertex taskVertex, AtlasTask task) throws Exception {
            TaskFactory  factory      = taskTypeFactoryMap.get(task.getType());
            if (factory == null) {
                LOG.error("taskTypeFactoryMap does not contain task of type: {}", task.getType());
                return;
            }

            AbstractTask runnableTask = factory.create(task);

            LOG.info("Starting task {}: {}", task.getGuid(), task.getType());
            registry.inProgress(taskVertex, task);

            runnableTask.run();

            registry.complete(taskVertex, task);

            statistics.successPrint();
        }
    }

    /*static class TaskQueueWatcher implements Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(TaskQueueWatcher.class);
        private static final TaskExecutor.TaskLogger TASK_LOG         = TaskExecutor.TaskLogger.getLogger();

        private final TaskRegistry registry;
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
            LOG.info("TaskQueueWatcher: running {}:{}", Thread.currentThread().getName(), Thread.currentThread().getId());
            try {
                while (true) {

                    if (latch != null && latch.getCount() != 0) {
                        LOG.info("TaskQueueWatcher: Waiting for Latch to complete {}", latch.getCount());
                        latch.await();
                    }

                    if (latch != null) {
                        LOG.info("TaskQueueWatcher: Latch wait complete {}", latch.getCount());
                    }

                    //fetch
                    //List<AtlasTask> tasks = registry.getTasksForReQueue();
                    new Thread(() -> {
                        LOG.info("in different thread{}", registry.getTasksForReQueue().size());
                    }).start();

                    //LOG.info("TaskQueueWatcher: Fetched next {} tasks", tasks.size());

                    //addAll(tasks);
                    //LOG.info("TaskQueueWatcher: Submitted next {} tasks", tasks.size());

                    Thread.sleep(15000);
                }
            } catch (InterruptedException e) {
                LOG.error("Await on latch interrupted");
            }
        }

        private void addAll(List<AtlasTask> tasks) {
            latch = new CountDownLatch(tasks.size());

            for (AtlasTask task : tasks) {
                if (task == null) {
                    continue;
                }

                TASK_LOG.log(task);

                this.executorService.submit(new TaskExecutor.TaskConsumer(task, this.registry, this.taskTypeFactoryMap, this.statistics, latch));
            }
        }
    }*/

    static class TaskLogger {
        private static final Logger LOG = LoggerFactory.getLogger("TASKS");

        public static TaskLogger getLogger() {
            return new TaskLogger();
        }

        public void info(String message) {
            LOG.info(message);
        }

        public void log(AtlasTask task) {
            LOG.info(AtlasType.toJson(task));
        }

        public void warn(String message, AtlasTask task) {
            LOG.warn(message, AtlasType.toJson(task));
        }

        public void error(String s, AtlasTask task, Exception exception) {
            LOG.error(s, AtlasType.toJson(task), exception);
        }
    }
}