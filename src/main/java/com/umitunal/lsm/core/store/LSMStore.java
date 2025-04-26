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

package com.umitunal.lsm.core.store;

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.api.Storage;
import com.umitunal.lsm.core.backgroundservice.CleanupService;
import com.umitunal.lsm.core.backgroundservice.CompactionService;
import com.umitunal.lsm.core.backgroundservice.FlushService;
import com.umitunal.lsm.core.compaction.CompactionStrategyType;
import com.umitunal.lsm.config.LSMStoreConfig;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;
import com.umitunal.lsm.wal.WAL;
import com.umitunal.lsm.wal.WALImpl;
import com.umitunal.lsm.wal.record.DeleteRecord;
import com.umitunal.lsm.wal.record.PutRecord;
import com.umitunal.lsm.wal.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Implementation of a Log-Structured Merge (LSM) tree storage engine.
 * 
 * <p>The LSM tree is a data structure designed for high write throughput. It maintains data in multiple
 * components: an in-memory component (MemTable) for recent writes, and multiple on-disk components (SSTables)
 * for older data. Writes are first logged to a Write-Ahead Log (WAL) for durability, then applied to the
 * MemTable. When the MemTable fills up, it's made immutable and flushed to disk as an SSTable.</p>
 * 
 * <p>Background services handle maintenance tasks like flushing MemTables to disk, compacting SSTables
 * to reclaim space, and cleaning up expired data.</p>
 * 
 * <p>This implementation is thread-safe and supports concurrent reads and writes.</p>
 */
public class LSMStore implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(LSMStore.class);

    // Configuration parameters
    private final LSMStoreConfig config;

    // Core components
    private MemTable activeMemTable;
    private final List<SSTable> ssTables;
    private final List<MemTable> immutableMemTables;
    private WAL wal;

    // Concurrency control
    private final ReadWriteLock memTableLock;
    private final AtomicLong sequenceNumber;
    private final AtomicBoolean recovering;

    // Background services
    private final CompactionService compactionService;
    private final CleanupService cleanupService;
    private final FlushService flushService;

    // Operation stores
    private PutStore putStore;
    private GetStore getStore;
    private DeleteStore deleteStore;

    /**
     * Creates a new LSMStore with default configuration.
     */
    public LSMStore() {
        this(LSMStoreConfig.getDefault());
    }

    /**
     * Creates a new LSMStore with custom configuration.
     *
     * @param memTableMaxSizeBytes maximum size of the MemTable before flushing to disk
     * @param dataDirectory        directory to store SSTable files
     * @param compactionThreshold  number of SSTables that triggers compaction
     */
    public LSMStore(int memTableMaxSizeBytes, String dataDirectory, int compactionThreshold) {
        this(new LSMStoreConfig(
                memTableMaxSizeBytes,
                dataDirectory,
                compactionThreshold,
                30, // 30 minutes compaction interval
                1,  // 1 minute cleanup interval
                10, // 10 seconds flush interval
                CompactionStrategyType.THRESHOLD // Default to threshold-based compaction
        ));
    }

    /**
     * Creates a new LSMStore with custom configuration and a specific compaction strategy.
     *
     * @param memTableMaxSizeBytes   maximum size of the MemTable before flushing to disk
     * @param dataDirectory          directory to store SSTable files
     * @param compactionThreshold    number of SSTables that triggers compaction
     * @param compactionStrategyType the type of compaction strategy to use
     */
    public LSMStore(int memTableMaxSizeBytes, String dataDirectory, int compactionThreshold,
                    CompactionStrategyType compactionStrategyType) {
        this(new LSMStoreConfig(
                memTableMaxSizeBytes,
                dataDirectory,
                compactionThreshold,
                30, // 30 minutes compaction interval
                1,  // 1 minute cleanup interval
                10, // 10 seconds flush interval
                compactionStrategyType
        ));
    }

    /**
     * Creates a new LSMStore with the specified configuration.
     *
     * @param config the configuration for this LSMStore
     */
    public LSMStore(LSMStoreConfig config) {
        this.config = config;

        // Initialize components
        this.activeMemTable = new MemTable(config.memTableMaxSizeBytes());
        this.ssTables = new ArrayList<>();
        this.immutableMemTables = new ArrayList<>();

        // Initialize concurrency control
        this.memTableLock = new ReentrantReadWriteLock();
        this.sequenceNumber = new AtomicLong(0);
        this.recovering = new AtomicBoolean(false);

        // Create data directory if it doesn't exist
        File dir = new File(config.dataDirectory());
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // Load existing SSTables from disk
        loadSSTables();

        // Create WAL directory
        String walDirectory = Paths.get(config.dataDirectory(), "wal").toString();
        try {
            Files.createDirectories(Paths.get(walDirectory));

            // Initialize WAL
            this.wal = new WALImpl(walDirectory);

            // Recover from WAL if it exists
            recover();
        } catch (IOException e) {
            logger.error("Error initializing WAL", e);
        }

        // Initialize background services
        this.compactionService = new CompactionService(ssTables, config, sequenceNumber);
        this.cleanupService = new CleanupService(activeMemTable, immutableMemTables, memTableLock, config);
        this.flushService = new FlushService(immutableMemTables, ssTables, config, sequenceNumber, compactionService);

        // Start background services
        compactionService.start();
        cleanupService.start();
        flushService.start();

        // Initialize operation stores
        putStore = new PutStore(
                activeMemTable,
                memTableLock,
                wal,
                recovering
        );

        getStore = new GetStore(
                activeMemTable,
                immutableMemTables,
                ssTables,
                memTableLock
        );

        deleteStore = new DeleteStore(
                activeMemTable,
                memTableLock,
                wal,
                recovering,
                getStore
        );

        logger.info("LSMStore initialized with data directory: " + config.dataDirectory());
    }


    //--------------------------------------------------------------------------
    // Initialization and Recovery
    //--------------------------------------------------------------------------

    /**
     * Loads existing SSTables from disk.
     * This method scans the data directory for SSTable files and loads them into the ssTables list.
     */
    private void loadSSTables() {
        File dataDir = new File(config.dataDirectory());
        File[] files = dataDir.listFiles((dir, name) -> name.endsWith(".data"));

        if (files == null || files.length == 0) {
            logger.info("No SSTable files found in directory: " + config.dataDirectory());
            return;
        }

        logger.info("Loading " + files.length + " SSTable files from disk");

        for (File file : files) {
            String fileName = file.getName();
            // Parse the level and sequence number from the file name
            // Format: sst_L<level>_S<sequenceNumber>.data
            if (fileName.startsWith("sst_L")) {
                try {
                    int levelStart = fileName.indexOf('L') + 1;
                    int levelEnd = fileName.indexOf('_', levelStart);
                    int level = Integer.parseInt(fileName.substring(levelStart, levelEnd));

                    int seqStart = fileName.indexOf('S') + 1;
                    int seqEnd = fileName.indexOf('.', seqStart);
                    long seq = Long.parseLong(fileName.substring(seqStart, seqEnd));

                    // Update the sequence number to be greater than the highest in the SSTables
                    sequenceNumber.updateAndGet(current -> Math.max(current, seq + 1));

                    // Load the SSTable
                    SSTable ssTable = new SSTable(config.dataDirectory(), level, seq);
                    ssTables.add(ssTable);

                    logger.info("Loaded SSTable: " + fileName);
                } catch (Exception e) {
                    logger.error("Error loading SSTable: " + fileName, e);
                }
            }
        }

        // Sort SSTables by sequence number (newest first)
        ssTables.sort((a, b) -> Long.compare(b.getSequenceNumber(), a.getSequenceNumber()));

        logger.info("Loaded " + ssTables.size() + " SSTables from disk");
    }

    /**
     * Recovers the state of the database from the WAL.
     * This method is called during startup to replay the WAL and restore the state of the MemTable.
     * 
     * <p>Recovery process:</p>
     * <ol>
     *   <li>Read all records from the WAL</li>
     *   <li>Sort records by sequence number to ensure correct order</li>
     *   <li>Update the sequence number to be greater than the highest in the WAL</li>
     *   <li>Replay records by applying them to the active MemTable</li>
     * </ol>
     * 
     * @throws IOException if an error occurs while reading from the WAL (handled internally)
     */
    private void recover() {
        try {
            // Set recovering flag to true
            recovering.set(true);

            // Read all records from the WAL
            List<Record> records = wal.readRecords();

            if (records.isEmpty()) {
                logger.info("No WAL records to recover");
                return;
            }

            logger.info("""
                    Recovering %d records from WAL
                    """.formatted(records.size()));

            // Sort records by sequence number to ensure correct order
            records.sort((r1, r2) -> Long.compare(r1.getSequenceNumber(), r2.getSequenceNumber()));

            // Update sequence number to be greater than the highest in the WAL
            if (!records.isEmpty()) {
                long maxSeqNum = records.get(records.size() - 1).getSequenceNumber();
                sequenceNumber.set(maxSeqNum + 1);
            }

            // Replay records
            for (Record record : records) {
                if (record instanceof PutRecord putRecord) {
                    activeMemTable.put(putRecord.getKey(), putRecord.getValue(), putRecord.getTtlSeconds());
                } else if (record instanceof DeleteRecord) {
                    activeMemTable.delete(record.getKey());
                }
            }

            logger.info("""
                    Recovery completed
                    """);
        } catch (IOException e) {
            logger.error("Error recovering from WAL", e);
        } finally {
            // Reset recovering flag
            recovering.set(false);
        }
    }

    //--------------------------------------------------------------------------
    // MemTable Management and Maintenance
    //--------------------------------------------------------------------------

    /**
     * Switches to a new MemTable when the current one is full.
     * The current MemTable becomes immutable and is scheduled for flushing to disk.
     * 
     * <p>This method performs the following steps:</p>
     * <ol>
     *   <li>Make the current MemTable immutable</li>
     *   <li>Add it to the list of immutable MemTables</li>
     *   <li>Create a new active MemTable</li>
     *   <li>Update the GetStore, PutStore, and DeleteStore with the new state</li>
     *   <li>Trigger an immediate flush of the immutable MemTable to disk</li>
     * </ol>
     * 
     * <p>This method acquires a write lock on the MemTable to ensure thread safety.</p>
     */
    private void switchMemTable() {
        memTableLock.writeLock().lock();
        try {
            // Make the current MemTable immutable
            activeMemTable.makeImmutable();

            // Add it to the list of immutable MemTables
            synchronized (immutableMemTables) {
                immutableMemTables.add(activeMemTable);
            }

            // Create a new active MemTable
            activeMemTable = new MemTable(config.memTableMaxSizeBytes());

            // Update the GetStore and PutStore with the new state
            getStore.updateDependencies(
                    activeMemTable,
                    immutableMemTables,
                    ssTables,
                    memTableLock
            );

            putStore.updateDependencies(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering
            );

            deleteStore.updateDependencies(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering,
                    getStore
            );

            logger.info("Switched to new MemTable, old one scheduled for flushing");

            // Trigger an immediate flush to ensure data is persisted to disk
            flushMemTables();
        } finally {
            memTableLock.writeLock().unlock();
        }
    }

    //--------------------------------------------------------------------------
    // Core Operations (put, get, delete)
    //--------------------------------------------------------------------------

    /**
     * Stores a key-value pair with a time-to-live (TTL) in the LSM store.
     * 
     * <p>The operation is first delegated to the PutStore. If the MemTable is full,
     * this method switches to a new MemTable and retries the operation.</p>
     * 
     * <p>The put operation follows these steps:</p>
     * <ol>
     *   <li>Write the operation to the WAL for durability</li>
     *   <li>Add the key-value pair to the active MemTable</li>
     *   <li>If the MemTable is full, switch to a new one and retry</li>
     * </ol>
     *
     * @param key the key as a byte array
     * @param value the value as a byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     * @return true if the operation was successful, false otherwise
     */
    @Override
    public boolean put(byte[] key, byte[] value, long ttlSeconds) {
        // Delegate to PutStore
        int result = putStore.put(key, value, ttlSeconds);

        // Handle the result
        if (result == PutStore.RESULT_SUCCESS) {
            return true;
        } else if (result == PutStore.RESULT_MEMTABLE_FULL) {
            // Switch to a new MemTable and try again
            switchMemTable();

            // Create a new PutStore with the updated activeMemTable
            putStore = new PutStore(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering
            );

            // Try again with the new MemTable
            int newResult = putStore.put(key, value, ttlSeconds);
            return newResult == PutStore.RESULT_SUCCESS;
        } else {
            return false;
        }
    }

    /**
     * Stores a key-value pair in the LSM store with no expiration time.
     * 
     * <p>This is a convenience method that calls {@link #put(byte[], byte[], long)} with a TTL of 0,
     * which means the entry will not expire.</p>
     *
     * @param key the key as a byte array
     * @param value the value as a byte array
     * @return true if the operation was successful, false otherwise
     * @see #put(byte[], byte[], long)
     */
    @Override
    public boolean put(byte[] key, byte[] value) {
        return put(key, value, 0); // 0 means no expiration
    }

    /**
     * Retrieves the value associated with the specified key.
     * 
     * <p>The operation is delegated to the GetStore, which searches for the key in the following order:</p>
     * <ol>
     *   <li>Active MemTable (most recent data)</li>
     *   <li>Immutable MemTables (from newest to oldest)</li>
     *   <li>SSTables (from newest to oldest)</li>
     * </ol>
     * 
     * <p>This search order ensures that the most recent value for a key is returned.</p>
     *
     * @param key the key as a byte array
     * @return the value as a byte array, or null if the key was not found or is expired/deleted
     */
    @Override
    public byte[] get(byte[] key) {
        // Delegate to GetStore
        int result = getStore.get(key);

        // Handle the result
        if (result == GetStore.RESULT_SUCCESS) {
            return getStore.getRetrievedValue();
        } else {
            return null;
        }
    }

    /**
     * Deletes the key-value pair associated with the specified key.
     * 
     * <p>In LSM-tree storage, deletes are implemented as "tombstones" - special markers that indicate
     * a key has been deleted. The actual data is removed during compaction.</p>
     * 
     * <p>The operation is delegated to the DeleteStore. If the MemTable is full,
     * this method switches to a new MemTable and retries the operation.</p>
     * 
     * <p>The delete operation follows these steps:</p>
     * <ol>
     *   <li>Check if the key exists using GetStore</li>
     *   <li>Write the delete operation to the WAL for durability</li>
     *   <li>Add a tombstone marker to the active MemTable</li>
     *   <li>If the MemTable is full, switch to a new one and retry</li>
     * </ol>
     *
     * @param key the key as a byte array
     * @return true if the operation was successful, false if the key was not found or an error occurred
     */
    @Override
    public boolean delete(byte[] key) {
        // Delegate to DeleteStore
        int result = deleteStore.delete(key);

        // Handle the result
        if (result == DeleteStore.RESULT_SUCCESS) {
            return true;
        } else if (result == DeleteStore.RESULT_MEMTABLE_FULL) {
            // Switch to a new MemTable and try again
            switchMemTable();

            // Create a new DeleteStore with the updated activeMemTable
            DeleteStore newDeleteStore = new DeleteStore(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering,
                    getStore
            );

            // Try again with the new MemTable
            int newResult = newDeleteStore.delete(key);
            return newResult == DeleteStore.RESULT_SUCCESS;
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------
    // Query Operations (listKeys, containsKey, size, getRange, getIterator)
    //--------------------------------------------------------------------------

    @Override
    public List<byte[]> listKeys() {
        List<byte[]> allKeys = new ArrayList<>();

        // Collect keys from active MemTable
        memTableLock.readLock().lock();
        try {
            Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
            for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
                if (!entry.getValue().isExpired() && !entry.getValue().isTombstone()) {
                    allKeys.add(entry.getKey().getData());
                }
            }
        } finally {
            memTableLock.readLock().unlock();
        }

        // Collect keys from immutable MemTables
        synchronized (immutableMemTables) {
            for (MemTable memTable : immutableMemTables) {
                Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
                for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
                    if (!entry.getValue().isExpired() && !entry.getValue().isTombstone()) {
                        allKeys.add(entry.getKey().getData());
                    }
                }
            }
        }

        // Collect keys from SSTables
        synchronized (ssTables) {
            for (SSTable ssTable : ssTables) {
                List<byte[]> tableKeys = ssTable.listKeys();
                for (byte[] key : tableKeys) {
                    // We need to check if the key is already in the list to avoid duplicates
                    // This is inefficient for large datasets, but it's a simple implementation
                    boolean isDuplicate = false;
                    for (byte[] existingKey : allKeys) {
                        if (Arrays.equals(key, existingKey)) {
                            isDuplicate = true;
                            break;
                        }
                    }
                    if (!isDuplicate) {
                        allKeys.add(key);
                    }
                }
            }
        }

        return allKeys;
    }

    @Override
    public boolean containsKey(byte[] key) {
        if (key == null || key.length == 0) {
            return false;
        }

        // Delegate to GetStore
        int result = getStore.get(key);

        // If the result is Success, the key exists
        return result == GetStore.RESULT_SUCCESS;
    }

    @Override
    public int size() {
        int totalSize = 0;

        // Count entries in active MemTable
        memTableLock.readLock().lock();
        try {
            Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
            for (MemTable.ValueEntry entry : entries.values()) {
                if (!entry.isExpired() && !entry.isTombstone()) {
                    totalSize++;
                }
            }
        } finally {
            memTableLock.readLock().unlock();
        }

        // Count entries in immutable MemTables
        synchronized (immutableMemTables) {
            for (MemTable memTable : immutableMemTables) {
                Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
                for (MemTable.ValueEntry entry : entries.values()) {
                    if (!entry.isExpired() && !entry.isTombstone()) {
                        totalSize++;
                    }
                }
            }
        }

        // Count entries in SSTables
        // This is an approximation as it doesn't account for duplicates across SSTables
        // or entries that might be overridden by newer entries in MemTables
        synchronized (ssTables) {
            for (SSTable ssTable : ssTables) {
                totalSize += ssTable.countEntries();
            }
        }

        return totalSize;
    }

    //--------------------------------------------------------------------------
    // Lifecycle Operations (clear, shutdown)
    //--------------------------------------------------------------------------

    @Override
    public void clear() {
        // Clear active MemTable
        memTableLock.writeLock().lock();
        try {
            activeMemTable.close();
            activeMemTable = new MemTable(config.memTableMaxSizeBytes());
        } finally {
            memTableLock.writeLock().unlock();
        }

        // Clear immutable MemTables
        synchronized (immutableMemTables) {
            for (MemTable memTable : immutableMemTables) {
                memTable.close();
            }
            immutableMemTables.clear();
        }

        // Clear SSTables
        synchronized (ssTables) {
            for (SSTable ssTable : ssTables) {
                ssTable.delete();
            }
            ssTables.clear();
        }

        // Reset sequence number
        sequenceNumber.set(0);

        // Delete all WAL files
        try {
            wal.deleteAllLogs();
        } catch (IOException e) {
            logger.error("Error deleting WAL files", e);
        }

        // Update the GetStore and PutStore with the new state
        getStore.updateDependencies(
                activeMemTable,
                immutableMemTables,
                ssTables,
                memTableLock
        );

        putStore.updateDependencies(
                activeMemTable,
                memTableLock,
                wal,
                recovering
        );

        deleteStore.updateDependencies(
                activeMemTable,
                memTableLock,
                wal,
                recovering,
                getStore
        );

        logger.info("LSMStore cleared");
    }

    @Override
    public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
        // Use a custom Map implementation that can handle byte array keys
        Map<byte[], byte[]> result = new HashMap<byte[], byte[]>() {
            @Override
            public byte[] get(Object key) {
                if (!(key instanceof byte[] keyBytes)) {
                    return null;
                }
                for (Entry<byte[], byte[]> entry : entrySet()) {
                    if (Arrays.equals(entry.getKey(), keyBytes)) {
                        return entry.getValue();
                    }
                }

                return null;
            }
        };

        // Use the iterator for efficient range scanning
        try (KeyValueIterator iterator = getIterator(startKey, endKey)) {
            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                result.put(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            logger.error("Error during range query", e);
        }

        return result;
    }

    @Override
    public KeyValueIterator getIterator(byte[] startKey, byte[] endKey) {
        return new LSMStoreIterator(startKey, endKey);
    }

    /**
     * Gets the number of entries in the active MemTable.
     * This method is used for testing purposes.
     * 
     * @return the number of entries in the active MemTable
     */
    public int getActiveMemTableEntryCount() {
        return activeMemTable.getEntries().size();
    }

    /**
     * Gets the number of immutable MemTables.
     * This method is used for testing purposes.
     * 
     * @return the number of immutable MemTables
     */
    public int getImmutableMemTablesCount() {
        synchronized (immutableMemTables) {
            return immutableMemTables.size();
        }
    }

    /**
     * Gets the number of SSTables.
     * This method is used for testing purposes.
     * 
     * @return the number of SSTables
     */
    public int getSSTablesCount() {
        synchronized (ssTables) {
            return ssTables.size();
        }
    }

    /**
     * Forces a flush of all immutable MemTables to disk as SSTables.
     * This is useful for testing and for ensuring that all data is persisted.
     */
    public void flushMemTables() {
        System.out.println("[DEBUG_LOG] LSMStore.flushMemTables called");

        // Log the number of immutable MemTables
        synchronized (immutableMemTables) {
            System.out.println("[DEBUG_LOG] Number of immutable MemTables: " + immutableMemTables.size());
        }

        // Flush any pending data
        flushService.executeNow();

        // Log the number of SSTables
        synchronized (ssTables) {
            System.out.println("[DEBUG_LOG] Number of SSTables after flush: " + ssTables.size());

            // Log the SSTable files
            for (SSTable ssTable : ssTables) {
                System.out.println("[DEBUG_LOG] SSTable: " + ssTable.getSequenceNumber());
            }
        }
    }

    /**
     * Switches to a new MemTable for testing purposes.
     * This method is used by tests to make the active MemTable immutable and add it to the list of immutable MemTables.
     * It's similar to switchMemTable() but doesn't trigger a flush.
     */
    public void switchMemTableForTest() {
        System.out.println("[DEBUG_LOG] LSMStore.switchMemTableForTest called");

        // Log the active MemTable size and entries
        System.out.println("[DEBUG_LOG] Active MemTable size: " + activeMemTable.getSizeBytes() + " bytes, entries: " + activeMemTable.getEntries().size());

        memTableLock.writeLock().lock();
        try {
            // Make the current MemTable immutable
            activeMemTable.makeImmutable();
            System.out.println("[DEBUG_LOG] Made active MemTable immutable");

            // Add it to the list of immutable MemTables
            synchronized (immutableMemTables) {
                immutableMemTables.add(activeMemTable);
                System.out.println("[DEBUG_LOG] Added active MemTable to immutable list, total: " + immutableMemTables.size());
            }

            // Create a new active MemTable
            activeMemTable = new MemTable(config.memTableMaxSizeBytes());
            System.out.println("[DEBUG_LOG] Created new active MemTable");

            // Update the GetStore and PutStore with the new state
            getStore.updateDependencies(
                    activeMemTable,
                    immutableMemTables,
                    ssTables,
                    memTableLock
            );

            putStore.updateDependencies(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering
            );

            deleteStore.updateDependencies(
                    activeMemTable,
                    memTableLock,
                    wal,
                    recovering,
                    getStore
            );

            logger.info("Switched to new MemTable for testing");
            System.out.println("[DEBUG_LOG] Updated dependencies");
        } finally {
            memTableLock.writeLock().unlock();
        }
    }

    //--------------------------------------------------------------------------
    // Iterator Implementation
    //--------------------------------------------------------------------------

    /**
     * Iterator implementation for LSMStore that merges results from MemTable and SSTables.
     */
    private class LSMStoreIterator implements KeyValueIterator {
        private final ByteArrayWrapper startKeyWrapper;
        private final ByteArrayWrapper endKeyWrapper;
        private final List<Map.Entry<ByteArrayWrapper, byte[]>> entries;
        private int currentIndex = 0;

        public LSMStoreIterator(byte[] startKey, byte[] endKey) {
            this.startKeyWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
            this.endKeyWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;
            this.entries = new ArrayList<>();

            // Collect entries from MemTables and SSTables
            collectEntries();

            // Sort entries by key
            Collections.sort(entries, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
        }

        private void collectEntries() {
            // Collect entries from active MemTable
            memTableLock.readLock().lock();
            try {
                Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = activeMemTable.getEntries();
                for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
                    ByteArrayWrapper key = entry.getKey();

                    // Check if key is in range
                    if (isKeyInRange(key)) {
                        MemTable.ValueEntry valueEntry = entry.getValue();

                        // Skip expired or tombstone entries
                        if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
                            entries.add(new AbstractMap.SimpleEntry<>(key, valueEntry.getValue()));
                        }
                    }
                }
            } finally {
                memTableLock.readLock().unlock();
            }

            // Collect entries from immutable MemTables
            synchronized (immutableMemTables) {
                for (MemTable memTable : immutableMemTables) {
                    Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = memTable.getEntries();
                    for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
                        ByteArrayWrapper key = entry.getKey();

                        // Check if key is in range
                        if (isKeyInRange(key)) {
                            MemTable.ValueEntry valueEntry = entry.getValue();

                            // Skip expired or tombstone entries
                            if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
                                entries.add(new AbstractMap.SimpleEntry<>(key, valueEntry.getValue()));
                            }
                        }
                    }
                }
            }

            // Collect entries from SSTables
            synchronized (ssTables) {
                for (SSTable ssTable : ssTables) {
                    // Get entries in the specified range from the SSTable
                    byte[] startKeyBytes = startKeyWrapper != null ? startKeyWrapper.getData() : null;
                    byte[] endKeyBytes = endKeyWrapper != null ? endKeyWrapper.getData() : null;

                    Map<byte[], byte[]> rangeEntries = ssTable.getRange(startKeyBytes, endKeyBytes);

                    for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
                        byte[] key = entry.getKey();
                        byte[] value = entry.getValue();

                        // Skip entries that are already in the list (from MemTables)
                        boolean isDuplicate = false;
                        for (Map.Entry<ByteArrayWrapper, byte[]> existingEntry : entries) {
                            if (Arrays.equals(key, existingEntry.getKey().getData())) {
                                isDuplicate = true;
                                break;
                            }
                        }

                        if (!isDuplicate) {
                            entries.add(new AbstractMap.SimpleEntry<>(new ByteArrayWrapper(key), value));
                        }
                    }
                }
            }
        }

        private boolean isKeyInRange(ByteArrayWrapper key) {
            if (startKeyWrapper != null && key.compareTo(startKeyWrapper) < 0) {
                return false;
            }
            if (endKeyWrapper != null && key.compareTo(endKeyWrapper) >= 0) {
                return false;
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < entries.size();
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in the iterator");
            }

            Map.Entry<ByteArrayWrapper, byte[]> entry = entries.get(currentIndex++);
            return new AbstractMap.SimpleEntry<>(entry.getKey().getData(), entry.getValue());
        }

        @Override
        public byte[] peekNextKey() {
            if (!hasNext()) {
                return null;
            }

            return entries.get(currentIndex).getKey().getData();
        }

        @Override
        public void close() {
            // No resources to release
        }
    }

    @Override
    public void shutdown() {
        try {
            // Flush any pending data
            flushService.executeNow();

            // Make active MemTable immutable and flush it
            memTableLock.writeLock().lock();
            try {
                activeMemTable.makeImmutable();
                synchronized (immutableMemTables) {
                    immutableMemTables.add(activeMemTable);
                }
            } finally {
                memTableLock.writeLock().unlock();
            }

            flushService.executeNow();

            // Shutdown background services
            flushService.shutdown();
            compactionService.shutdown();
            cleanupService.shutdown();

            if (!flushService.awaitTermination(5, TimeUnit.SECONDS)) {
                flushService.shutdownNow();
            }
            if (!compactionService.awaitTermination(5, TimeUnit.SECONDS)) {
                compactionService.shutdownNow();
            }
            if (!cleanupService.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupService.shutdownNow();
            }

            // Close all SSTables
            synchronized (ssTables) {
                for (SSTable ssTable : ssTables) {
                    ssTable.close();
                }
            }

            // Close the WAL
            try {
                if (wal != null) {
                    wal.close();
                }
            } catch (IOException e) {
                logger.warn("Error closing WAL", e);
            }

            logger.info("LSMStore shutdown completed");
        } catch (InterruptedException e) {
            flushService.shutdownNow();
            compactionService.shutdownNow();
            cleanupService.shutdownNow();
            Thread.currentThread().interrupt();
            logger.warn("LSMStore shutdown interrupted", e);
        }
    }
}
