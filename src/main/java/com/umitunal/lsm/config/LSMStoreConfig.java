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

package com.umitunal.lsm.config;

import com.umitunal.lsm.core.compaction.CompactionStrategyType;

/**
 * Configuration record for LSMStore.
 * This record encapsulates all configuration parameters for an LSMStore instance.
 *
 * @param memTableMaxSizeBytes maximum size of the MemTable before flushing to disk
 * @param dataDirectory directory to store SSTable files
 * @param compactionThreshold number of SSTables that triggers compaction
 * @param compactionIntervalMinutes interval in minutes between compaction runs
 * @param cleanupIntervalMinutes interval in minutes between TTL cleanup runs
 * @param flushIntervalSeconds interval in seconds between MemTable flush runs
 * @param compactionStrategyType the type of compaction strategy to use
 */
public record LSMStoreConfig(
    int memTableMaxSizeBytes, 
    String dataDirectory, 
    int compactionThreshold,
    int compactionIntervalMinutes,
    int cleanupIntervalMinutes,
    int flushIntervalSeconds,
    CompactionStrategyType compactionStrategyType
) {

    /**
     * Creates a new LSMStoreConfig with the specified parameters.
     * Validates that all parameters are valid.
     *
     * @throws IllegalArgumentException if any parameter is invalid
     */
    public LSMStoreConfig {
        if (memTableMaxSizeBytes <= 0) {
            throw new IllegalArgumentException("memTableMaxSizeBytes must be positive");
        }
        if (dataDirectory == null || dataDirectory.isEmpty()) {
            throw new IllegalArgumentException("dataDirectory must not be null or empty");
        }
        if (compactionThreshold <= 0) {
            throw new IllegalArgumentException("compactionThreshold must be positive");
        }
        if (compactionIntervalMinutes <= 0) {
            throw new IllegalArgumentException("compactionIntervalMinutes must be positive");
        }
        if (cleanupIntervalMinutes <= 0) {
            throw new IllegalArgumentException("cleanupIntervalMinutes must be positive");
        }
        if (flushIntervalSeconds <= 0) {
            throw new IllegalArgumentException("flushIntervalSeconds must be positive");
        }
        if (compactionStrategyType == null) {
            throw new IllegalArgumentException("compactionStrategyType must not be null");
        }
    }

    /**
     * Creates a default configuration with:
     * - 10MB MemTable size
     * - "./data" data directory
     * - 4 SSTables before compaction
     * - 30 minutes compaction interval
     * - 1 minute cleanup interval
     * - 10 seconds flush interval
     * - THRESHOLD compaction strategy
     *
     * @return a default configuration
     */
    public static LSMStoreConfig getDefault() {
        return new LSMStoreConfig(10 * 1024 * 1024, "./data", 4, 30, 1, 10, CompactionStrategyType.THRESHOLD);
    }

    /**
     * Creates a new configuration with a custom MemTable size.
     *
     * @param memTableMaxSizeBytes maximum size of the MemTable before flushing to disk
     * @return a new configuration with the specified MemTable size
     */
    public LSMStoreConfig withMemTableMaxSizeBytes(int memTableMaxSizeBytes) {
        return new LSMStoreConfig(
            memTableMaxSizeBytes, 
            this.dataDirectory, 
            this.compactionThreshold,
            this.compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom data directory.
     *
     * @param dataDirectory directory to store SSTable files
     * @return a new configuration with the specified data directory
     */
    public LSMStoreConfig withDataDirectory(String dataDirectory) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            dataDirectory, 
            this.compactionThreshold,
            this.compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom compaction threshold.
     *
     * @param compactionThreshold number of SSTables that triggers compaction
     * @return a new configuration with the specified compaction threshold
     */
    public LSMStoreConfig withCompactionThreshold(int compactionThreshold) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            this.dataDirectory, 
            compactionThreshold,
            this.compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom compaction interval.
     *
     * @param compactionIntervalMinutes interval in minutes between compaction runs
     * @return a new configuration with the specified compaction interval
     */
    public LSMStoreConfig withCompactionIntervalMinutes(int compactionIntervalMinutes) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            this.dataDirectory, 
            this.compactionThreshold,
            compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom cleanup interval.
     *
     * @param cleanupIntervalMinutes interval in minutes between TTL cleanup runs
     * @return a new configuration with the specified cleanup interval
     */
    public LSMStoreConfig withCleanupIntervalMinutes(int cleanupIntervalMinutes) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            this.dataDirectory, 
            this.compactionThreshold,
            this.compactionIntervalMinutes,
            cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom flush interval.
     *
     * @param flushIntervalSeconds interval in seconds between MemTable flush runs
     * @return a new configuration with the specified flush interval
     */
    public LSMStoreConfig withFlushIntervalSeconds(int flushIntervalSeconds) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            this.dataDirectory, 
            this.compactionThreshold,
            this.compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            flushIntervalSeconds,
            this.compactionStrategyType
        );
    }

    /**
     * Creates a new configuration with a custom compaction strategy type.
     *
     * @param compactionStrategyType the type of compaction strategy to use
     * @return a new configuration with the specified compaction strategy type
     */
    public LSMStoreConfig withCompactionStrategyType(CompactionStrategyType compactionStrategyType) {
        return new LSMStoreConfig(
            this.memTableMaxSizeBytes, 
            this.dataDirectory, 
            this.compactionThreshold,
            this.compactionIntervalMinutes,
            this.cleanupIntervalMinutes,
            this.flushIntervalSeconds,
            compactionStrategyType
        );
    }
}
