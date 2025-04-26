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

import java.util.Arrays;
import java.util.Map;

/**
 * Record representing a key-value entry.
 * This immutable record stores a key-value pair for use in iterators and other operations.
 *
 * @param key the key as byte array
 * @param value the value as byte array
 */
public record KeyValueEntry(byte[] key, byte[] value) implements Map.Entry<byte[], byte[]> {

    /**
     * Creates a new KeyValueEntry with the specified key and value.
     * Makes defensive copies of the key and value to ensure immutability.
     */
    public KeyValueEntry {
        if (key != null) {
            key = key.clone();
        }
        if (value != null) {
            value = value.clone();
        }
    }

    /**
     * Returns a copy of the key to prevent external modification.
     *
     * @return a copy of the key
     */
    @Override
    public byte[] getKey() {
        return key != null ? key.clone() : null;
    }

    /**
     * Returns a copy of the value to prevent external modification.
     *
     * @return a copy of the value
     */
    @Override
    public byte[] getValue() {
        return value != null ? value.clone() : null;
    }

    /**
     * This operation is not supported as KeyValueEntry is immutable.
     *
     * @param value the new value
     * @return never returns
     * @throws UnsupportedOperationException always
     */
    @Override
    public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException("KeyValueEntry is immutable");
    }

    /**
     * Compares this entry with another object for equality.
     * Two entries are equal if they have the same key and value.
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueEntry that = (KeyValueEntry) o;
        return Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
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
        return result;
    }

    /**
     * Returns a string representation of this entry.
     *
     * @return a string representation of this entry
     */
    @Override
    public String toString() {
        return "KeyValueEntry[" +
               "key=" + Arrays.toString(key) +
               ", value=" + Arrays.toString(value) +
               ']';
    }
}