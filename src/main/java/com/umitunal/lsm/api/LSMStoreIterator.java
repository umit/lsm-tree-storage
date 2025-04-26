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

package com.umitunal.lsm.api;

import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Interface for LSMStore iterators.
 * This interface extends the KeyValueIterator interface and adds methods specific to LSM store iteration.
 */
public interface LSMStoreIterator extends KeyValueIterator {

    /**
     * Implementation of LSMStoreIterator that iterates over entries in a MemTable.
     */
    class MemTableIterator implements LSMStoreIterator {
        private final List<Map.Entry<ByteArrayWrapper, byte[]>> entries;
        private int currentIndex = 0;

        /**
         * Creates a new MemTableIterator for the specified MemTable and range.
         *
         * @param memTable the MemTable to iterate over
         * @param startKey the start key (inclusive), or null for the first key
         * @param endKey the end key (exclusive), or null for no upper bound
         * @param memTableLock the lock to use for thread safety
         */
        public MemTableIterator(MemTable memTable, byte[] startKey, byte[] endKey, ReadWriteLock memTableLock) {
            ByteArrayWrapper startKeyWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
            ByteArrayWrapper endKeyWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;
            this.entries = new ArrayList<>();

            // Collect entries from MemTable
            memTableLock.readLock().lock();
            try {
                Map<ByteArrayWrapper, MemTable.ValueEntry> memEntries = memTable.getEntries();
                for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memEntries.entrySet()) {
                    ByteArrayWrapper key = entry.getKey();

                    // Check if key is in range
                    if (isKeyInRange(key, startKeyWrapper, endKeyWrapper)) {
                        MemTable.ValueEntry valueEntry = entry.getValue();

                        // Skip expired or tombstone entries
                        if (!valueEntry.isExpired() && !valueEntry.isTombstone()) {
                            entries.add(Map.entry(key, valueEntry.getValue()));
                        }
                    }
                }
            } finally {
                memTableLock.readLock().unlock();
            }

            // Sort entries by key
            Collections.sort(entries, (e1, e2) -> e1.getKey().compareTo(e2.getKey()));
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
            return new KeyValueEntry(entry.getKey().getData(), entry.getValue());
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

        /**
         * Checks if a key is within the specified range.
         * 
         * <p>A key is in range if it is greater than or equal to the start key (if specified)
         * and less than the end key (if specified).</p>
         * 
         * @param key the key to check
         * @param startKey the inclusive lower bound, or null for no lower bound
         * @param endKey the exclusive upper bound, or null for no upper bound
         * @return true if the key is in range, false otherwise
         */
        private boolean isKeyInRange(ByteArrayWrapper key, ByteArrayWrapper startKey, ByteArrayWrapper endKey) {
            if (startKey != null && key.compareTo(startKey) < 0) {
                return false;
            }
            if (endKey != null && key.compareTo(endKey) >= 0) {
                return false;
            }
            return true;
        }
    }

    /**
     * Implementation of LSMStoreIterator that iterates over entries in an SSTable.
     */
    class SSTableIterator implements LSMStoreIterator {
        private final List<Map.Entry<byte[], byte[]>> entries;
        private int currentIndex = 0;

        /**
         * Creates a new SSTableIterator for the specified SSTable and range.
         *
         * @param ssTable the SSTable to iterate over
         * @param startKey the start key (inclusive), or null for the first key
         * @param endKey the end key (exclusive), or null for no upper bound
         */
        public SSTableIterator(SSTable ssTable, byte[] startKey, byte[] endKey) {
            // Get entries in the specified range from the SSTable
            Map<byte[], byte[]> rangeEntries = ssTable.getRange(startKey, endKey);

            // Convert to a list for iteration
            this.entries = new ArrayList<>();
            for (Map.Entry<byte[], byte[]> entry : rangeEntries.entrySet()) {
                entries.add(new KeyValueEntry(entry.getKey(), entry.getValue()));
            }

            // Sort entries by key
            Collections.sort(entries, (e1, e2) -> {
                ByteArrayWrapper w1 = new ByteArrayWrapper(e1.getKey());
                ByteArrayWrapper w2 = new ByteArrayWrapper(e2.getKey());
                return w1.compareTo(w2);
            });
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

            return entries.get(currentIndex++);
        }

        @Override
        public byte[] peekNextKey() {
            if (!hasNext()) {
                return null;
            }

            return entries.get(currentIndex).getKey();
        }

        @Override
        public void close() {
            // No resources to release
        }
    }

    /**
     * Implementation of LSMStoreIterator that merges results from multiple iterators.
     */
    class MergedIterator implements LSMStoreIterator {
        private final List<LSMStoreIterator> iterators;
        private final ByteArrayWrapper startKey;
        private final ByteArrayWrapper endKey;
        private Map.Entry<byte[], byte[]> nextEntry;

        /**
         * Creates a new MergedIterator that merges results from the specified iterators.
         *
         * @param iterators the iterators to merge
         * @param startKey the start key (inclusive), or null for the first key
         * @param endKey the end key (exclusive), or null for no upper bound
         */
        public MergedIterator(List<LSMStoreIterator> iterators, byte[] startKey, byte[] endKey) {
            this.iterators = new ArrayList<>(iterators);
            this.startKey = startKey != null ? new ByteArrayWrapper(startKey) : null;
            this.endKey = endKey != null ? new ByteArrayWrapper(endKey) : null;

            // Initialize by finding the first entry
            findNextEntry();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in the iterator");
            }

            Map.Entry<byte[], byte[]> current = nextEntry;
            findNextEntry();
            return current;
        }

        @Override
        public byte[] peekNextKey() {
            return hasNext() ? nextEntry.getKey() : null;
        }

        @Override
        public void close() {
            for (LSMStoreIterator iterator : iterators) {
                iterator.close();
            }
        }

        /**
         * Finds the next entry to return from the merged iterators.
         * 
         * <p>This method implements the core logic of the merged iterator by finding the smallest
         * key among all the underlying iterators. When multiple iterators have the same key,
         * entries from earlier iterators in the list take precedence (which aligns with the
         * LSM-tree property that newer data overwrites older data).</p>
         * 
         * <p>The method also skips entries with the same key from other iterators to avoid
         * returning duplicate keys.</p>
         */
        private void findNextEntry() {
            nextEntry = null;

            // Find the iterator with the smallest next key
            ByteArrayWrapper smallestKey = null;
            LSMStoreIterator iteratorWithSmallestKey = null;

            for (LSMStoreIterator iterator : iterators) {
                if (iterator.hasNext()) {
                    byte[] key = iterator.peekNextKey();
                    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

                    // Check if key is in range
                    if (isKeyInRange(keyWrapper)) {
                        if (smallestKey == null || keyWrapper.compareTo(smallestKey) < 0) {
                            smallestKey = keyWrapper;
                            iteratorWithSmallestKey = iterator;
                        }
                    }
                }
            }

            // If we found an iterator with a valid next key, get the next entry
            if (iteratorWithSmallestKey != null) {
                nextEntry = iteratorWithSmallestKey.next();

                // Skip entries with the same key from other iterators
                // (newer entries from earlier iterators take precedence)
                for (LSMStoreIterator iterator : iterators) {
                    if (iterator != iteratorWithSmallestKey && iterator.hasNext()) {
                        byte[] key = iterator.peekNextKey();
                        if (Arrays.equals(key, nextEntry.getKey())) {
                            iterator.next(); // Skip this entry
                        }
                    }
                }
            }
        }

        /**
         * Checks if a key is within the specified range.
         * 
         * <p>A key is in range if it is greater than or equal to the start key (if specified)
         * and less than the end key (if specified).</p>
         * 
         * @param key the key to check
         * @return true if the key is in range, false otherwise
         */
        private boolean isKeyInRange(ByteArrayWrapper key) {
            if (startKey != null && key.compareTo(startKey) < 0) {
                return false;
            }
            if (endKey != null && key.compareTo(endKey) >= 0) {
                return false;
            }
            return true;
        }
    }
}
