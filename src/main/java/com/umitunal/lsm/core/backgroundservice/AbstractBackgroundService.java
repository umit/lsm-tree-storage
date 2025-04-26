/*
 * Copyright (c) 2023-2025 Umit Unal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.umitunal.lsm.core.backgroundservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for background services in the LSM store.
 * Provides common functionality for scheduling and executing periodic tasks.
 */
public abstract class AbstractBackgroundService implements BackgroundService {

    protected final Logger logger;
    protected final ScheduledExecutorService executorService;
    protected final String serviceName;

    /**
     * Creates a new background service with the specified name.
     * 
     * @param serviceName the name of the service, used for logging and thread naming
     */
    protected AbstractBackgroundService(String serviceName) {
        this.serviceName = serviceName;
        this.logger = LoggerFactory.getLogger(this.getClass());

        // Create a virtual thread executor for better scalability
        this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread.Builder.OfVirtual builder = Thread.ofVirtual().name("LSMStore-" + serviceName);
            return builder.factory().newThread(r);
        });
    }

    /**
     * Schedules the service's task to run periodically.
     * 
     * @param initialDelaySeconds delay before first execution
     * @param periodSeconds time between executions
     * @param timeUnit the time unit of the initialDelay and period parameters
     */
    protected void scheduleTask(long initialDelaySeconds, long periodSeconds, TimeUnit timeUnit) {
        executorService.scheduleAtFixedRate(
            this::executeTask, 
            initialDelaySeconds, 
            periodSeconds, 
            timeUnit
        );
        logger.info(serviceName + " service scheduled to run every " + periodSeconds + " " + 
                    timeUnit.toString().toLowerCase());
    }

    /**
     * Executes the service's task and handles any exceptions.
     * This method is called by the scheduler.
     */
    private void executeTask() {
        try {
            executeNow();
        } catch (Exception e) {
            logger.error("Error during " + serviceName + " execution", e);
        }
    }

    @Override
    public void executeNow() {
        try {
            logger.debug(serviceName + " service executing");
            doExecute();
            logger.debug(serviceName + " service completed");
        } catch (Exception e) {
            logger.error("Error during " + serviceName + " execution", e);
        }
    }

    /**
     * Implements the actual task logic.
     * This method should be implemented by subclasses.
     */
    protected abstract void doExecute();


    @Override
    public void shutdown() {
        logger.info(serviceName + " service shutting down");
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn(serviceName + " service did not terminate in time, forcing shutdown");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn(serviceName + " service shutdown interrupted, forcing shutdown");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    @Override
    public void shutdownNow() {
        logger.info(serviceName + " service shutting down now");
        executorService.shutdownNow();
    }
}
