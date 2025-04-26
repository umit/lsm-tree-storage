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

package com.umitunal.lsm.sstable.index;

import com.umitunal.lsm.api.ByteArrayWrapper;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Interface for managing SSTable index files.
 * This interface defines methods for reading and writing index entries to the index file.
 */
public interface IndexFileManager extends AutoCloseable {
    
    /**
     * Gets the path of the index file.
     * 
     * @return the path of the index file
     */
    Path getIndexFilePath();
    
    /**
     * Gets the file channel for the index file.
     * 
     * @return the file channel
     */
    FileChannel getIndexChannel();
    
    /**
     * Adds an index entry.
     * 
     * @param key the key as byte array
     * @param offset the offset in the data file
     * @throws IOException if an I/O error occurs
     */
    void addIndexEntry(byte[] key, long offset) throws IOException;
    
    /**
     * Writes the index to the index file.
     * 
     * @throws IOException if an I/O error occurs
     */
    void writeIndex() throws IOException;
    
    /**
     * Loads the index from the index file.
     * 
     * @throws IOException if an I/O error occurs
     */
    void loadIndex() throws IOException;
    
    /**
     * Gets the sparse index.
     * 
     * @return the sparse index mapping keys to offsets in the data file
     */
    NavigableMap<ByteArrayWrapper, Long> getSparseIndex();
    
    /**
     * Finds the closest key in the sparse index that is less than or equal to the given key.
     * 
     * @param key the key to find
     * @return the entry with the closest key and its offset, or null if no such key exists
     */
    Map.Entry<ByteArrayWrapper, Long> findClosestKey(byte[] key);
    
    /**
     * Closes the index file and releases resources.
     * 
     * @throws IOException if an I/O error occurs
     */
    @Override
    void close() throws IOException;
}