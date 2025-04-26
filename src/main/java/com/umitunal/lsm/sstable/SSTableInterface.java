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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for SSTable operations.
 * This interface defines the contract for SSTable implementations.
 */
public interface SSTableInterface extends AutoCloseable {
    
    /**
     * Gets a value by key.
     * 
     * @param key the key as byte array
     * @return the value as byte array, or null if the key doesn't exist
     */
    byte[] get(byte[] key);
    
    /**
     * Checks if a key might be in this SSTable.
     * This is a fast check using the Bloom filter, which may return false positives
     * but never false negatives.
     * 
     * @param key the key as byte array
     * @return true if the key might be in this SSTable, false if it definitely isn't
     */
    boolean mightContain(byte[] key);
    
    /**
     * Gets the level of this SSTable in the LSM-tree.
     * 
     * @return the level
     */
    int getLevel();
    
    /**
     * Gets the sequence number of this SSTable.
     * 
     * @return the sequence number
     */
    long getSequenceNumber();
    
    /**
     * Gets the creation time of this SSTable.
     * 
     * @return the creation time in milliseconds since the epoch
     */
    long getCreationTime();
    
    /**
     * Gets the size of this SSTable in bytes.
     * 
     * @return the size in bytes
     */
    long getSizeBytes();
    
    /**
     * Deletes all files associated with this SSTable.
     * 
     * @return true if all files were deleted successfully, false otherwise
     */
    boolean delete();
    
    /**
     * Lists all keys in this SSTable.
     * 
     * @return a list of all keys in this SSTable, excluding tombstones
     */
    List<byte[]> listKeys();
    
    /**
     * Counts the number of entries in this SSTable.
     * 
     * @return the number of entries in this SSTable
     */
    int countEntries();
    
    /**
     * Gets all key-value pairs with keys in the specified range.
     * The range is inclusive of startKey and exclusive of endKey.
     * 
     * @param startKey the start of the range (inclusive), or null for the first key
     * @param endKey the end of the range (exclusive), or null for no upper bound
     * @return a map of keys to values in the specified range
     */
    Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey);
    
    /**
     * Closes this SSTable and releases resources.
     * 
     * @throws IOException if an I/O error occurs
     */
    @Override
    void close() throws IOException;
}