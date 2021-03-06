/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.dirtail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DirTailSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger                        logger               = LoggerFactory.getLogger(DirTailSource.class);

    private DirPattern                                 dirPattern           = new DirPattern();
    private SourceCounter                              sourceCounter;
    private ExecutorService                            executor;
    private Integer                                    bufferCount;
    private long                                       batchTimeout;
    private Charset                                    charset;
    private Map<String, Pair<ExecRunnable, Future<?>>> runningMap           = new HashMap<String, Pair<ExecRunnable, Future<?>>>();
    private FileSystemMonitor                          fsm;
    private boolean                                    topicByFileName      = false;
    private boolean                                    splitFileName2Header = false;
    private boolean                                    restart;
    private long                                       restartThrottle;

    @Override
    public void start() {
        logger.info("Dir tail source starting :" + dirPattern.getPath());
        executor = Executors.newFixedThreadPool(1000);
        sourceCounter.start();
        super.start();
        logger.debug("Dir tail source started");
        fsm = new FileSystemMonitor(this, dirPattern);
        logger.info("DirTailSource init finished . ");
    }

    @Override
    public void stop() {
        logger.info("Stopping dir tail  source" + dirPattern.getPath());
        fsm.stop();
        for (Map.Entry<String, Pair<ExecRunnable, Future<?>>> e : runningMap.entrySet()) {
            e.getValue().getLeft().setRestart(false);
            e.getValue().getLeft().kill();
            e.getValue().getRight().cancel(true);
        }
        runningMap.clear();
        executor.shutdown();
        while (!executor.isTerminated()) {
            logger.debug("Waiting for dir executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for dir executor service " + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        sourceCounter.stop();
        super.stop();
        logger.debug("DirTair source with path:{} stopped. Metrics:{}", dirPattern.getPath(), sourceCounter);
    }

    @Override
    public void configure(Context context) {
        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE, ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT, ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);
        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET, ExecSourceConfigurationConstants.DEFAULT_CHARSET));
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        Preconditions.checkState(context.getString("dirPath") != null, "The parameter dir must be specified");
        dirPattern.setPath(context.getString("dirPath"));
        dirPattern.setFilePattern(context.getString("file-pattern", "^(.*)$"));
        topicByFileName = context.getBoolean("topicByFileName", false);
        splitFileName2Header = context.getBoolean("splitFileName2Header", false);
        restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART, ExecSourceConfigurationConstants.DEFAULT_RESTART);
        restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE, ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);
    }

    public void commitTask(String path, String fileName, boolean fromHead) {
        if (runningMap.containsKey(path))
            return;
        logger.info("add task " + path);
        ExecRunnable runner =
                new ExecRunnable(path, fromHead, getChannelProcessor(), sourceCounter, bufferCount, batchTimeout, charset, fileName, topicByFileName, splitFileName2Header,
                        restart, restartThrottle);
        runningMap.put(path, new Pair<DirTailSource.ExecRunnable, Future<?>>(runner, executor.submit(runner)));
    }

    public void removeTask(String path) {
        if (runningMap.containsKey(path)) {
            logger.info("remove task " + path);
            runningMap.get(path).getLeft().setRestart(false);
            runningMap.get(path).getLeft().kill();
            runningMap.get(path).getRight().cancel(true);
            runningMap.remove(path);
        }
    }

    private static class ExecRunnable implements Runnable {

        public ExecRunnable(String path, boolean fromHead, ChannelProcessor channelProcessor, SourceCounter sourceCounter, int bufferCount, long batchTimeout, Charset charset,
                String fileName, boolean topicByFileName, boolean splitFileName2Header, boolean restart, long restartThrottle) {
            this.commandbasic = "tail -F -n ";
            this.channelProcessor = channelProcessor;
            this.sourceCounter = sourceCounter;
            this.bufferCount = bufferCount;
            this.batchTimeout = batchTimeout;
            this.charset = charset;
            this.path = path;
            this.fileName = fileName;
            this.fromHead = fromHead;
            this.topicByFileName = topicByFileName;
            this.splitFileName2Header = splitFileName2Header;
            this.restart = restart;
            this.restartThrottle = restartThrottle;
        }

        private final String           commandbasic;
        private String                 command;
        private final ChannelProcessor channelProcessor;
        private final SourceCounter    sourceCounter;
        private final int              bufferCount;
        private long                   batchTimeout;
        private final Charset          charset;
        private Process                process           = null;
        private SystemClock            systemClock       = new SystemClock();
        private Long                   lastPushToChannel = systemClock.currentTimeMillis();
        ScheduledExecutorService       timedFlushService;
        ScheduledFuture<?>             future;
        private String                 path;
        private String                 fileName;
        boolean                        fromHead;
        private boolean                topicByFileName;
        private boolean                splitFileName2Header;
        private volatile boolean       restart;
        private long                   restartThrottle;

        @Override
        public void run() {
            String topic = null, type = null;
            if (splitFileName2Header) {
                if (fileName.contains(".") && fileName.contains("_") && fileName.indexOf(".") > fileName.indexOf("_")) {
                    topic = fileName.substring(0, fileName.indexOf("_"));
                    type = fileName.substring(fileName.indexOf("_") + 1, fileName.indexOf("."));
                } else {
                    logger.warn("splitFileName2Header Failed : " + fileName);
                }
            }

            BufferedReader reader = null;
            String line = null;
            final List<Event> eventList = new ArrayList<Event>();
            timedFlushService =
                    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("timedFlushExecService" + Thread.currentThread().getId() + "-%d").build());
            do {
                try {
                    command = commandbasic + (fromHead ? "+0 " : "0 ") + path;
                    fromHead = false;
                    String[] commandArgs = command.split("\\s+");
                    process = new ProcessBuilder(commandArgs).start();
                    reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));
                    future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                synchronized (eventList) {
                                    if (!eventList.isEmpty() && timeout()) {
                                        flushEventBatch(eventList);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Exception occured when processing event batch", e);
                                if (e instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    }, batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);
                    while ((line = reader.readLine()) != null) {
                        synchronized (eventList) {
                            sourceCounter.incrementEventReceivedCount();
                            Event et = EventBuilder.withBody(line.getBytes(charset));
                            if (topicByFileName) {
                                et.getHeaders().put("topic", fileName);
                            }
                            if (splitFileName2Header && topic != null && type != null) {
                                et.getHeaders().put("topic", topic);
                                et.getHeaders().put("type", type);
                            }
                            eventList.add(et);
                            if (eventList.size() >= bufferCount || timeout()) {
                                flushEventBatch(eventList);
                            }
                        }
                    }
                    synchronized (eventList) {
                        if (!eventList.isEmpty()) {
                            flushEventBatch(eventList);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed while running command: " + command, e);
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("Failed to close reader for dir tail source", ex);
                        }
                    }
                    kill();
                }
                try {
                    Thread.sleep(restartThrottle);
                } catch (InterruptedException e) {
                }
            } while (restart);
        }

        private void flushEventBatch(List<Event> eventList) {
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            eventList.clear();
            lastPushToChannel = systemClock.currentTimeMillis();
        }

        private boolean timeout() {
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
        }

        public void setRestart(boolean restart) {
            this.restart = restart;
        }

        public int kill() {
            if (process != null) {
                synchronized (process) {
                    process.destroy();

                    try {
                        int exitValue = process.waitFor();
                        if (future != null) {
                            future.cancel(true);
                        }
                        if (timedFlushService != null) {
                            timedFlushService.shutdown();
                            while (!timedFlushService.isTerminated()) {
                                try {
                                    timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    logger.debug("Interrupted while waiting for dir tail executor service " + "to stop. Just exiting.");
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                        return exitValue;
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Integer.MIN_VALUE;
            }
            return Integer.MIN_VALUE / 2;
        }
    }
}
