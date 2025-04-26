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

package com.umitunal.lsm.wal.record;

import java.util.Arrays;

/**
 * Record for a put operation in the WAL.
 * This record contains a key, a value, and a TTL.
 */
public class PutRecord implements Record {
    private final long sequenceNumber;
    private final byte[] key;
    private final byte[] value;
    private final long ttlSeconds;
    
    /**
     * Creates a new PutRecord.
     * 
     * @param sequenceNumber the sequence number of this record
     * @param key the key as byte array
     * @param value the value as byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     */
    public PutRecord(long sequenceNumber, byte[] key, byte[] value, long ttlSeconds) {
        this.sequenceNumber = sequenceNumber;
        this.key = key.clone(); // Defensive copy
        this.value = value.clone(); // Defensive copy
        this.ttlSeconds = ttlSeconds;
    }
    
    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    @Override
    public byte[] getKey() {
        return key.clone(); // Return a copy to prevent modification
    }
    
    /**
     * Gets the value associated with this record.
     * 
     * @return the value as byte array
     */
    public byte[] getValue() {
        return value.clone(); // Return a copy to prevent modification
    }
    
    /**
     * Gets the TTL in seconds.
     * 
     * @return the TTL in seconds, 0 means no expiration
     */
    public long getTtlSeconds() {
        return ttlSeconds;
    }
    
    @Override
    public String toString() {
        return "PutRecord{" +
                "sequenceNumber=" + sequenceNumber +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                ", ttlSeconds=" + ttlSeconds +
                '}';
    }
}