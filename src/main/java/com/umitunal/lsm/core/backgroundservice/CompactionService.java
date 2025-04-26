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

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.config.LSMStoreConfig;
import com.umitunal.lsm.core.compaction.CompactionStrategy;
import com.umitunal.lsm.core.compaction.CompactionStrategyType;
import com.umitunal.lsm.core.compaction.SizeTieredCompactionStrategy;
import com.umitunal.lsm.core.compaction.ThresholdCompactionStrategy;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background service that performs compaction of SSTables to optimize storage and query performance.
 * 
 * <p>Compaction is a critical process in LSM-tree storage systems that merges multiple SSTables into a single,
 * larger SSTable. This process has several important benefits:</p>
 * <ul>
 *   <li>Reduces the number of SSTables, improving read performance by reducing the number of files to search</li>
 *   <li>Removes deleted data (tombstones) and obsolete versions of data</li>
 *   <li>Reclaims disk space by removing duplicate and deleted data</li>
 *   <li>Organizes data into levels, improving the efficiency of range queries</li>
 * </ul>
 * 
 * <p>This service uses a configurable compaction strategy to determine which SSTables to compact and when.
 * Different strategies can be used depending on the workload characteristics:</p>
 * <ul>
 *   <li>Threshold-based: Compacts when the number of SSTables exceeds a threshold</li>
 *   <li>Size-tiered: Groups SSTables by size and compacts when enough tables of similar size exist</li>
 * </ul>
 * 
 * <p>Compaction runs periodically in the background and can also be triggered manually when needed.</p>
 */
public class CompactionService extends AbstractBackgroundService {

    private final List<SSTable> ssTables;
    private final LSMStoreConfig config;
    private final AtomicLong sequenceNumber;
    private final CompactionStrategy compactionStrategy;

    /**
     * Creates a new compaction service.
     * 
     * @param ssTables the list of SSTables to compact
     * @param config the LSM store configuration
     * @param sequenceNumber the sequence number generator
     */
    public CompactionService(List<SSTable> ssTables, LSMStoreConfig config, AtomicLong sequenceNumber) {
        super("Compaction");
        this.ssTables = ssTables;
        this.config = config;
        this.sequenceNumber = sequenceNumber;
        this.compactionStrategy = createCompactionStrategy(config);
    }

    /**
     * Creates a compaction strategy based on the configuration.
     * 
     * <p>This method instantiates the appropriate compaction strategy implementation
     * based on the strategy type specified in the configuration. The available strategies are:</p>
     * <ul>
     *   <li>THRESHOLD: Uses the ThresholdCompactionStrategy, which compacts when the number of
     *       SSTables exceeds a threshold</li>
     *   <li>SIZE_TIERED: Uses the SizeTieredCompactionStrategy, which groups SSTables by size
     *       and compacts when enough tables of similar size exist</li>
     * </ul>
     * 
     * <p>The strategy selection affects how and when compaction is performed, which can
     * significantly impact the performance characteristics of the LSM store.</p>
     * 
     * @param config the LSM store configuration containing the compaction strategy type
     * @return the instantiated compaction strategy
     */
    private CompactionStrategy createCompactionStrategy(LSMStoreConfig config) {
        CompactionStrategyType strategyType = config.compactionStrategyType();

        return switch (strategyType) {
            case THRESHOLD -> new ThresholdCompactionStrategy(config);
            case SIZE_TIERED -> new SizeTieredCompactionStrategy(config);
        };
    }

    /**
     * Starts the compaction service.
     * 
     * <p>This method schedules the compaction task to run periodically according to the
     * configured interval. The initial delay is set to 30 seconds to allow the system
     * to stabilize before the first compaction runs.</p>
     * 
     * <p>The compaction interval is specified in the LSMStoreConfig and is typically
     * set to a value that balances the need for regular compaction with the performance
     * impact of running compaction too frequently.</p>
     */
    @Override
    public void start() {
        scheduleTask(30, config.compactionIntervalMinutes(), TimeUnit.MINUTES);
    }

    /**
     * Executes the compaction process.
     * 
     * <p>This method performs the following steps:</p>
     * <ol>
     *   <li>Check if compaction is needed using the configured compaction strategy</li>
     *   <li>Select SSTables to compact based on the strategy</li>
     *   <li>Merge the selected SSTables, with newer data taking precedence over older data</li>
     *   <li>Create a new SSTable with the merged data</li>
     *   <li>Add the new SSTable to the list and remove the old ones</li>
     * </ol>
     * 
     * <p>The method uses Java virtual threads to parallelize I/O operations during the merge process,
     * improving performance on systems with multiple cores or when dealing with I/O-bound operations.</p>
     * 
     * <p>All operations on the SSTable list are synchronized to ensure thread safety.</p>
     */
    @Override
    protected void doExecute() {
        try {
            synchronized (ssTables) {
                // Use the compaction strategy to determine if compaction should be performed
                if (!compactionStrategy.shouldCompact(ssTables)) {
                    logger.info("""
                       Skipping compaction, compaction strategy (%s) determined it's not needed
                       """.formatted(compactionStrategy.getName()));
                    return;
                }

                logger.info("""
                    Starting compaction process with %d SSTables using %s
                    """.formatted(ssTables.size(), compactionStrategy.getName()));

                // Use the compaction strategy to select the tables to compact
                List<SSTable> tablesToCompact = compactionStrategy.selectTableToCompact(ssTables);

                // Skip if no tables were selected for compaction
                if (tablesToCompact.isEmpty()) {
                    logger.info("Skipping compaction, no SSTables selected for compaction");
                    return;
                }

                // Get the level of the first table for logging purposes
                int level = tablesToCompact.get(0).getLevel();

                logger.info("""
                    Compacting %d SSTables at level %d
                    """.formatted(tablesToCompact.size(), level));

                // Create a temporary MemTable to merge the SSTables
                MemTable mergedMemTable = new MemTable(config.memTableMaxSizeBytes() * 10); // Larger size for merging

                // Merge the SSTables, with newer SSTables taking precedence over older ones
                // Sort by sequence number in descending order (newer first)
                tablesToCompact.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

                // Create a map to store the merged key-value pairs
                Map<ByteArrayWrapper, byte[]> mergedData = new HashMap<>();

                // Process SSTables in parallel using Java 21 virtual threads
                // This improves performance by parallelizing the I/O operations
                List<CompletableFuture<List<Map.Entry<byte[], byte[]>>>> futures = new ArrayList<>();

                // Create a virtual thread executor
                var executor = Executors.newVirtualThreadPerTaskExecutor();

                // Create a future for each SSTable
                for (SSTable ssTable : tablesToCompact) {
                    CompletableFuture<List<Map.Entry<byte[], byte[]>>> future = CompletableFuture.supplyAsync(
                        () -> getEntriesFromSSTable(ssTable),
                        executor
                    );
                    futures.add(future);
                }

                // Wait for all futures to complete and process the results
                for (int i = 0; i < tablesToCompact.size(); i++) {
                    SSTable ssTable = tablesToCompact.get(i);
                    try {
                        List<Map.Entry<byte[], byte[]>> entries = futures.get(i).get();

                        // If no entries are found, log a warning but continue
                        if (entries.isEmpty()) {
                            logger.warn("No entries found in SSTable with sequence number " + 
                                          ssTable.getSequenceNumber());
                        }

                        // Process the entries
                        for (Map.Entry<byte[], byte[]> entry : entries) {
                            ByteArrayWrapper key = new ByteArrayWrapper(entry.getKey());
                            // Only add if the key hasn't been seen yet (newer SSTables take precedence)
                            if (!mergedData.containsKey(key)) {
                                byte[] value = entry.getValue();
                                if (value != null) { // Skip tombstones
                                    mergedData.put(key, value);
                                }
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("Error processing SSTable during compaction", e);
                    }
                }

                // Shutdown the executor
                executor.close();

                // Check if we have any data to merge
                if (mergedData.isEmpty()) {
                    logger.warn("No data to merge during compaction - this is expected with the current implementation");
                    return;
                }

                // Add all merged data to the MemTable
                for (Map.Entry<ByteArrayWrapper, byte[]> entry : mergedData.entrySet()) {
                    mergedMemTable.put(entry.getKey().getData(), entry.getValue(), 0); // No TTL for simplicity
                }

                // Create a new SSTable at the output level determined by the compaction strategy
                int outputLevel = compactionStrategy.getCompactionOutputLevel(tablesToCompact);
                long newSequenceNumber = sequenceNumber.getAndIncrement();

                try {
                    SSTable compactedTable = new SSTable(mergedMemTable, config.dataDirectory(), outputLevel, newSequenceNumber);

                    // Add the new SSTable to the list
                    ssTables.add(compactedTable);

                    // Remove the old SSTables
                    for (SSTable ssTable : tablesToCompact) {
                        ssTables.remove(ssTable);
                        ssTable.delete(); // Delete the files
                        ssTable.close(); // Release resources
                    }

                    logger.info("""
                        Compaction completed. Created new SSTable at level %d with sequence number %d
                        """.formatted(outputLevel, newSequenceNumber));
                } catch (IOException e) {
                    logger.error("Error creating compacted SSTable", e);
                }
            }
        } catch (Exception e) {
            logger.error("Error during compaction", e);
        }
    }

    /**
     * Helper method to get all entries from an SSTable.
     * Uses the getRange method to efficiently retrieve all entries.
     * 
     * <p>This method retrieves all key-value pairs from the specified SSTable by calling
     * its getRange method with null start and end keys, which returns all entries. It then
     * filters out any entries with null values (which would be tombstones) and converts
     * the map entries to a list of SimpleEntry objects for easier processing.</p>
     * 
     * <p>This method is called by the doExecute method for each SSTable being compacted,
     * and the results are merged to create the new compacted SSTable.</p>
     * 
     * @param ssTable the SSTable to get entries from
     * @return a list of key-value pairs, excluding tombstones
     */
    private List<Map.Entry<byte[], byte[]>> getEntriesFromSSTable(SSTable ssTable) {
        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();

        // Get all entries from the SSTable using the getRange method
        Map<byte[], byte[]> rangeEntries = ssTable.getRange(null, null);

        // Convert the map entries to a list
        for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
            if (entry.getValue() != null) {
                entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
        }

        return entries;
    }
}
