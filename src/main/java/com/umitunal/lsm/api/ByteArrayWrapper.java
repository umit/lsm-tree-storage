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

/**
 * A wrapper for byte arrays that provides proper equals() and hashCode() implementations.
 * This class is used to safely use byte arrays as keys in maps and sets.
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
    private final byte[] data;

    /**
     * Creates a new ByteArrayWrapper with the given data.
     *
     * @param data the byte array to wrap
     */
    public ByteArrayWrapper(byte[] data) {
        if (data == null) {
            throw new NullPointerException("Data cannot be null");
        }
        this.data = data;
    }

    /**
     * Returns the wrapped byte array.
     *
     * @return the wrapped byte array
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Compares this ByteArrayWrapper with another ByteArrayWrapper.
     * The comparison is based on the lexicographical ordering of the byte arrays.
     *
     * @param other the ByteArrayWrapper to compare with
     * @return a negative integer, zero, or a positive integer as this object
     *         is less than, equal to, or greater than the specified object
     */
    @Override
    public int compareTo(ByteArrayWrapper other) {
        if (other == null) {
            return 1;
        }

        byte[] otherData = other.getData();
        int length = Math.min(data.length, otherData.length);

        for (int i = 0; i < length; i++) {
            int a = data[i] & 0xff;
            int b = otherData[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return data.length - otherData.length;
    }

    /**
     * Checks if this ByteArrayWrapper is equal to another object.
     * Two ByteArrayWrappers are equal if they wrap the same byte array content.
     *
     * @param obj the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ByteArrayWrapper other = (ByteArrayWrapper) obj;
        return Arrays.equals(data, other.data);
    }

    /**
     * Returns a hash code for this ByteArrayWrapper.
     * The hash code is based on the content of the wrapped byte array.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /**
     * Returns a string representation of this ByteArrayWrapper.
     * The string representation includes the content of the wrapped byte array.
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        return "ByteArrayWrapper{" +
                "data=" + Arrays.toString(data) +
                '}';
    }
}