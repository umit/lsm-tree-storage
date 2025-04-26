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

import com.umitunal.lsm.config.LSMStoreConfig;
import com.umitunal.lsm.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A compaction strategy that triggers compaction when the number of SSTables
 * exceeds a threshold. This is the original compaction strategy used in the LSM store.
 */
public class ThresholdCompactionStrategy implements CompactionStrategy {
    private static final Logger logger = LoggerFactory.getLogger(ThresholdCompactionStrategy.class);

    private final LSMStoreConfig config;

    /**
     * Creates a new ThresholdCompactionStrategy.
     * 
     * @param config the LSM store configuration
     */
    public ThresholdCompactionStrategy(LSMStoreConfig config) {
        this.config = config;
    }

    /**
     * Determines if compaction should be performed based on the number of SSTables.
     * 
     * <p>This strategy triggers compaction when the total number of SSTables
     * exceeds the threshold specified in the configuration.</p>
     * 
     * @param ssTables the list of SSTables to check
     * @return true if the number of SSTables exceeds the threshold, false otherwise
     */
    @Override
    public boolean shouldCompact(List<SSTable> ssTables) {
        return ssTables.size() >= config.compactionThreshold();
    }

    /**
     * Selects the SSTables that should be compacted.
     * 
     * <p>This method implements a level-based selection strategy:</p>
     * <ol>
     *   <li>Group SSTables by their level</li>
     *   <li>Find the level with the most SSTables</li>
     *   <li>Select all SSTables at that level for compaction</li>
     * </ol>
     * 
     * <p>The method requires at least 2 SSTables at a level to perform compaction.
     * If no level has at least 2 SSTables, an empty list is returned.</p>
     * 
     * @param ssTables the list of all SSTables
     * @return a list of SSTables to compact, or an empty list if no compaction is needed
     */
    @Override
    public List<SSTable> selectTableToCompact(List<SSTable> ssTables) {
        // Group SSTables by level
        Map<Integer, List<SSTable>> sstablesByLevel = new HashMap<>();
        for (SSTable ssTable : ssTables) {
            int level = ssTable.getLevel();
            sstablesByLevel.computeIfAbsent(level, k -> new ArrayList<>()).add(ssTable);
        }

        // Find the level with the most SSTables
        int levelToCompact = 0;
        int maxSSTables = 0;
        for (Map.Entry<Integer, List<SSTable>> entry : sstablesByLevel.entrySet()) {
            if (entry.getValue().size() > maxSSTables) {
                maxSSTables = entry.getValue().size();
                levelToCompact = entry.getKey();
            }
        }

        // Skip if the level doesn't have enough SSTables to compact
        List<SSTable> tablesToCompact = sstablesByLevel.get(levelToCompact);
        if (tablesToCompact == null || tablesToCompact.size() < 2) {
            logger.info("Skipping compaction, not enough SSTables at any level");
            return new ArrayList<>();
        }

        logger.info(String.format("Selected %d SSTables at level %d for compaction", 
                                 tablesToCompact.size(), levelToCompact));

        return tablesToCompact;
    }

    /**
     * Determines the level for the new compacted SSTable.
     * 
     * <p>This strategy uses a simple leveling approach where the output level
     * is one level higher than the input level. This helps organize data into
     * a tiered structure where higher levels contain older, less frequently
     * accessed data.</p>
     * 
     * <p>If the input list is empty, level 0 is returned as a default.</p>
     * 
     * @param tablesToCompact the list of SSTables being compacted
     * @return the level for the new compacted SSTable, which is the current level + 1
     */
    @Override
    public int getCompactionOutputLevel(List<SSTable> tablesToCompact) {
        // If there are no tables to compact, return level 0
        if (tablesToCompact.isEmpty()) {
            return 0;
        }

        // Get the level of the first table (they should all be at the same level)
        int currentLevel = tablesToCompact.get(0).getLevel();

        // The new level is the current level + 1
        return currentLevel + 1;
    }

    /**
     * Gets the name of this compaction strategy.
     * 
     * <p>This name is used for logging and debugging purposes.</p>
     * 
     * @return the name of the strategy
     */
    @Override
    public String getName() {
        return "ThresholdCompactionStrategy";
    }
}
