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

import com.umitunal.lsm.sstable.SSTable;

import java.util.List;

/**
 * Interface defining the contract for compaction strategies.
 * A compaction strategy determines which SSTables should be compacted and when.
 */
public interface CompactionStrategy {
    
    /**
     * Determines if compaction should be performed based on the current state of SSTables.
     * 
     * @param ssTables the list of SSTables
     * @return true if compaction should be performed, false otherwise
     */
    boolean shouldCompact(List<SSTable> ssTables);
    
    /**
     * Selects the SSTables that should be compacted.
     * 
     * @param ssTables the list of all SSTables
     * @return a list of SSTables to compact
     */
    List<SSTable> selectTableToCompact(List<SSTable> ssTables);
    
    /**
     * Determines the level for the new compacted SSTable.
     * 
     * @param tablesToCompact the list of SSTables being compacted
     * @return the level for the new compacted SSTable
     */
    int getCompactionOutputLevel(List<SSTable> tablesToCompact);
    
    /**
     * Gets the name of this compaction strategy.
     * 
     * @return the name of the strategy
     */
    String getName();
}