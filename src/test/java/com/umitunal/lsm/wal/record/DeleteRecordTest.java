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
 * Tests for the DeleteRecord class.
 */
class DeleteRecordTest {

    @Test
    void testConstructorAndGetters() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();

        // Act
        DeleteRecord record = new DeleteRecord(sequenceNumber, key);

        // Assert
        assertEquals(sequenceNumber, record.getSequenceNumber());
        assertArrayEquals(key, record.getKey());
    }

    @Test
    void testDefensiveCopies() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();

        // Act
        DeleteRecord record = new DeleteRecord(sequenceNumber, key);

        // Modify the original array
        key[0] = 'X';

        // Assert that the record's copy is not affected
        assertNotEquals(key[0], record.getKey()[0]);

        // Modify the array returned by getter
        byte[] returnedKey = record.getKey();
        returnedKey[0] = 'Z';

        // Assert that the record's internal state is not affected
        assertNotEquals(returnedKey[0], record.getKey()[0]);
    }

    @Test
    void testToString() {
        // Arrange
        long sequenceNumber = 123;
        byte[] key = "testKey".getBytes();

        // Act
        DeleteRecord record = new DeleteRecord(sequenceNumber, key);
        String toString = record.toString();

        // Assert
        assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
        assertTrue(toString.contains("key=" + Arrays.toString(key)));
    }
}