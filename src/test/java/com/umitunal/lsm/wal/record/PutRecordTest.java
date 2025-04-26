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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PutRecord class.
 */
class PutRecordTest {

    @Test
    void testConstructorAndGetters() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;

        // Act
        PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);

        // Assert
        assertEquals(sequenceNumber, record.getSequenceNumber());
        assertArrayEquals(key, record.getKey());
        assertArrayEquals(value, record.getValue());
        assertEquals(ttlSeconds, record.getTtlSeconds());
    }

    @Test
    void testDefensiveCopies() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;

        // Act
        PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);

        // Modify the original arrays
        key[0] = 'X';
        value[0] = 'Y';

        // Assert that the record's copies are not affected
        assertNotEquals(key[0], record.getKey()[0]);
        assertNotEquals(value[0], record.getValue()[0]);

        // Modify the arrays returned by getters
        byte[] returnedKey = record.getKey();
        byte[] returnedValue = record.getValue();
        returnedKey[0] = 'Z';
        returnedValue[0] = 'W';

        // Assert that the record's internal state is not affected
        assertNotEquals(returnedKey[0], record.getKey()[0]);
        assertNotEquals(returnedValue[0], record.getValue()[0]);
    }

    @Test
    void testToString() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;

        // Act
        PutRecord record = new PutRecord(sequenceNumber, key, value, ttlSeconds);
        String toString = record.toString();

        // Assert
        assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
        assertTrue(toString.contains("key=" + Arrays.toString(key)));
        assertTrue(toString.contains("value=" + Arrays.toString(value)));
        assertTrue(toString.contains("ttlSeconds=" + ttlSeconds));
    }
}