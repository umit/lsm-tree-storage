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

import java.util.List;
import java.util.Map;

/**
 * Interface for key-value storage operations.
 * This interface defines the basic operations that any storage implementation should support.
 */
public interface Storage {

    /**
     * Stores a key-value pair in the storage.
     *
     * @param key   the key as a byte array
     * @param value the value as a byte array
     * @return true if the operation was successful, false otherwise
     */
    boolean put(byte[] key, byte[] value);

    /**
     * Stores a key-value pair in the storage with a time-to-live (TTL).
     *
     * @param key        the key as a byte array
     * @param value      the value as a byte array
     * @param ttlSeconds the time-to-live in seconds, after which the entry will expire
     * @return true if the operation was successful, false otherwise
     */
    boolean put(byte[] key, byte[] value, long ttlSeconds);

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key the key as a byte array
     * @return the value as a byte array, or null if the key doesn't exist or is expired
     */
    byte[] get(byte[] key);

    /**
     * Deletes the entry with the given key.
     *
     * @param key the key as a byte array
     * @return true if the operation was successful, false otherwise
     */
    boolean delete(byte[] key);

    /**
     * Lists all keys in the storage.
     *
     * @return a list of all keys as byte arrays
     */
    List<byte[]> listKeys();

    /**
     * Checks if the storage contains the given key.
     *
     * @param key the key as a byte array
     * @return true if the key exists and is not expired, false otherwise
     */
    boolean containsKey(byte[] key);

    /**
     * Returns the number of entries in the storage.
     *
     * @return the number of entries
     */
    int size();

    /**
     * Clears all entries from the storage.
     */
    void clear();

    /**
     * Retrieves all key-value pairs within the given key range.
     *
     * @param startKey the start key (inclusive), or null for the first key
     * @param endKey   the end key (exclusive), or null for the last key
     * @return a map of keys to values within the range
     */
    Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey);

    /**
     * Returns an iterator over the key-value pairs within the given key range.
     *
     * @param startKey the start key (inclusive), or null for the first key
     * @param endKey   the end key (exclusive), or null for the last key
     * @return an iterator over the key-value pairs
     */
    KeyValueIterator getIterator(byte[] startKey, byte[] endKey);

    /**
     * Shuts down the storage, releasing any resources.
     */
    void shutdown();
}