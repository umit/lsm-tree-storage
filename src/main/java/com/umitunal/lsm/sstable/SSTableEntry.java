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

import java.util.Arrays;

/**
 * Record representing an entry in an SSTable.
 * This immutable record stores a key-value pair along with metadata.
 *
 * @param key the key as byte array
 * @param value the value as byte array
 * @param timestamp the timestamp when this entry was created
 * @param tombstone true if this is a tombstone marker (deletion)
 */
public record SSTableEntry(byte[] key, byte[] value, long timestamp, boolean tombstone) {

    /**
     * Compact constructor that makes defensive copies of the byte arrays.
     */
    public SSTableEntry {
        // Make defensive copies of the byte arrays
        if (key != null) {
            key = key.clone();
        }
        if (value != null) {
            value = value.clone();
        }
    }

    /**
     * Creates a new SSTableEntry for a key-value pair.
     *
     * @param key the key as byte array
     * @param value the value as byte array
     * @param timestamp the timestamp when this entry was created
     * @return a new SSTableEntry
     */
    public static SSTableEntry of(byte[] key, byte[] value, long timestamp) {
        return new SSTableEntry(key, value, timestamp, false);
    }

    /**
     * Creates a new tombstone SSTableEntry for a key.
     *
     * @param key the key as byte array
     * @param timestamp the timestamp when this entry was created
     * @return a new tombstone SSTableEntry
     */
    public static SSTableEntry tombstone(byte[] key, long timestamp) {
        return new SSTableEntry(key, null, timestamp, true);
    }

    /**
     * Checks if this entry is newer than another entry.
     *
     * @param other the other entry to compare with
     * @return true if this entry is newer than the other entry
     */
    public boolean isNewerThan(SSTableEntry other) {
        return this.timestamp > other.timestamp;
    }

    /**
     * Returns a copy of the key to prevent external modification.
     *
     * @return a copy of the key
     */
    @Override
    public byte[] key() {
        return key != null ? key.clone() : null;
    }

    /**
     * Returns a copy of the value to prevent external modification.
     *
     * @return a copy of the value
     */
    @Override
    public byte[] value() {
        return value != null ? value.clone() : null;
    }

    /**
     * Compares this entry with another object for equality.
     * Two entries are equal if they have the same key, value, timestamp, and tombstone flag.
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableEntry that = (SSTableEntry) o;
        return timestamp == that.timestamp &&
               tombstone == that.tombstone &&
               Arrays.equals(key, that.key) &&
               Arrays.equals(value, that.value);
    }

    /**
     * Returns a hash code for this entry.
     *
     * @return a hash code for this entry
     */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (tombstone ? 1 : 0);
        return result;
    }

    /**
     * Returns a string representation of this entry.
     *
     * @return a string representation of this entry
     */
    @Override
    public String toString() {
        return "SSTableEntry[" +
               "key=" + Arrays.toString(key) +
               ", value=" + Arrays.toString(value) +
               ", timestamp=" + timestamp +
               ", tombstone=" + tombstone +
               ']';
    }
}
