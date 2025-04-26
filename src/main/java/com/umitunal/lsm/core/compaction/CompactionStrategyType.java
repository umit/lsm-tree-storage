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

package com.umitunal.lsm.core.compaction;

/**
 * Enum defining the available compaction strategies.
 */
public enum CompactionStrategyType {
    /**
     * The original threshold-based compaction strategy.
     * Triggers compaction when the number of SSTables exceeds a threshold.
     */
    THRESHOLD,
    
    /**
     * Size-tiered compaction strategy.
     * Groups SSTables by size and compacts SSTables of similar size together.
     * More efficient for workloads with varying SSTable sizes.
     */
    SIZE_TIERED
}