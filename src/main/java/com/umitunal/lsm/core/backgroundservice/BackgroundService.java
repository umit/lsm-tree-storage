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

import java.util.concurrent.TimeUnit;

/**
 * Interface for background services in the LSM store.
 * Background services perform periodic tasks to maintain the store's health and performance.
 */
public interface BackgroundService {

    /**
     * Starts the background service.
     * This method should initialize any resources needed and schedule the periodic task.
     */
    void start();


    /**
     * Executes the service's task immediately, regardless of the schedule.
     * This is useful for testing or when immediate execution is needed.
     */
    void executeNow();

    /**
     * Shuts down the background service.
     * This is a convenience method that calls shutdown() on the executor service.
     */
    void shutdown();

    /**
     * Waits for the background service to terminate.
     * 
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if the service terminated, false if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to stop all actively executing tasks and halts the processing of waiting tasks.
     * This is a convenience method that calls shutdownNow() on the executor service.
     * 
     * <p>This method differs from {@link #shutdown()} in that it attempts to stop all actively
     * executing tasks, while shutdown only prevents new tasks from being submitted.</p>
     */
    void shutdownNow();
}
