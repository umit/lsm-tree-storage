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

package com.umitunal.lsm.sstable;

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.api.KeyValueIterator;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Sealed interface for SSTable iterators.
 * This interface extends the KeyValueIterator interface and adds methods specific to SSTable iteration.
 */
public sealed interface SSTableIterator extends KeyValueIterator permits SSTableIterator.InMemoryIterator, SSTableIterator.FileIterator {

    /**
     * Gets the current entry without advancing the iterator.
     *
     * @return the current SSTableEntry
     * @throws NoSuchElementException if there are no more entries
     */
    SSTableEntry currentEntry() throws NoSuchElementException;

    /**
     * Implementation of SSTableIterator that iterates over in-memory entries.
     */
    final class InMemoryIterator implements SSTableIterator {
        private final SSTableEntry[] entries;
        private int currentIndex = 0;

        /**
         * Creates a new InMemoryIterator with the specified entries.
         *
         * @param entries the entries to iterate over
         * @throws NullPointerException if entries is null
         */
        public InMemoryIterator(SSTableEntry[] entries) {
            if (entries == null) {
                throw new NullPointerException("Entries array cannot be null");
            }
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < entries.length;
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries");
            }

            SSTableEntry entry = entries[currentIndex++];
            return new AbstractMap.SimpleEntry<>(entry.key(), entry.value());
        }

        @Override
        public byte[] peekNextKey() {
            if (!hasNext()) {
                return null;
            }

            return entries[currentIndex].key();
        }

        @Override
        public void close() {
            // Nothing to close for in-memory iterator
        }

        @Override
        public SSTableEntry currentEntry() throws NoSuchElementException {
            if (currentIndex <= 0 || currentIndex > entries.length) {
                throw new NoSuchElementException("No current entry");
            }

            return entries[currentIndex - 1];
        }
    }

    /**
     * Implementation of SSTableIterator that iterates over entries in a file.
     */
    final class FileIterator implements SSTableIterator {
        private final SSTable ssTable;
        private final byte[] startKey;
        private final byte[] endKey;
        private SSTableEntry currentEntry;
        private boolean hasNext;

        /**
         * Creates a new FileIterator with the specified parameters.
         *
         * @param ssTable the SSTable to iterate over
         * @param startKey the start key (inclusive), or null for the first key
         * @param endKey the end key (exclusive), or null for no upper bound
         * @throws IOException if an I/O error occurs
         */
        public FileIterator(SSTable ssTable, byte[] startKey, byte[] endKey) throws IOException {
            this.ssTable = ssTable;
            this.startKey = startKey;
            this.endKey = endKey;

            // Initialize the iterator
            // This is a placeholder implementation
            // The actual implementation would read the first entry from the file
            this.hasNext = false;
            this.currentEntry = null;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries");
            }

            SSTableEntry entry = currentEntry;

            // Read the next entry
            // This is a placeholder implementation
            // The actual implementation would read the next entry from the file
            hasNext = false;
            currentEntry = null;

            return new AbstractMap.SimpleEntry<>(entry.key(), entry.value());
        }

        @Override
        public byte[] peekNextKey() {
            if (!hasNext()) {
                return null;
            }

            return currentEntry.key();
        }

        @Override
        public void close() {
            // Close any resources
            // This is a placeholder implementation
            // The actual implementation would close any open file handles
        }

        @Override
        public SSTableEntry currentEntry() throws NoSuchElementException {
            if (currentEntry == null) {
                throw new NoSuchElementException("No current entry");
            }

            return currentEntry;
        }
    }
}
