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

package com.umitunal.lsm.core;

import com.umitunal.lsm.core.store.PutStore;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.wal.WAL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class PutStoreTest {

    private PutStore putStore;
    private MemTable mockMemTable;
    private ReadWriteLock readWriteLock;
    private WAL mockWal;
    private AtomicBoolean recovering;

    @BeforeEach
    void setUp() {
        mockMemTable = Mockito.mock(MemTable.class);
        readWriteLock = new ReentrantReadWriteLock();
        mockWal = Mockito.mock(WAL.class);
        recovering = new AtomicBoolean(false);

        putStore = new PutStore(
            mockMemTable,
            readWriteLock,
            mockWal,
            recovering
        );
    }

    @Test
    void testPutWithNullKey() {
        int result = putStore.put(null, "value".getBytes(), 0);
        assertEquals(PutStore.RESULT_INVALID_INPUT, result);
        verifyNoInteractions(mockWal, mockMemTable);
    }

    @Test
    void testPutWithEmptyKey() {
        int result = putStore.put(new byte[0], "value".getBytes(), 0);
        assertEquals(PutStore.RESULT_INVALID_INPUT, result);
        verifyNoInteractions(mockWal, mockMemTable);
    }

    @Test
    void testPutWithNullValue() {
        int result = putStore.put("key".getBytes(), null, 0);
        assertEquals(PutStore.RESULT_INVALID_INPUT, result);
        verifyNoInteractions(mockWal, mockMemTable);
    }

    @Test
    void testSuccessfulPut() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        long ttl = 60;

        when(mockMemTable.put(key, value, ttl)).thenReturn(true);

        int result = putStore.put(key, value, ttl);

        assertEquals(PutStore.RESULT_SUCCESS, result);
        verify(mockWal).appendPutRecord(key, value, ttl);
        verify(mockMemTable).put(key, value, ttl);
    }

    @Test
    void testPutWithWALError() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        long ttl = 60;

        doThrow(new IOException("WAL error")).when(mockWal).appendPutRecord(key, value, ttl);

        int result = putStore.put(key, value, ttl);

        assertEquals(PutStore.RESULT_WAL_ERROR, result);
        verify(mockWal).appendPutRecord(key, value, ttl);
        verifyNoInteractions(mockMemTable);
        assertNotNull(putStore.getLastException());
    }

    @Test
    void testPutWithFullMemTable() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        long ttl = 60;

        when(mockMemTable.put(key, value, ttl)).thenReturn(false);
        when(mockMemTable.isFull()).thenReturn(true);

        int result = putStore.put(key, value, ttl);

        assertEquals(PutStore.RESULT_MEMTABLE_FULL, result);
        verify(mockWal).appendPutRecord(key, value, ttl);
        verify(mockMemTable).put(key, value, ttl);
        verify(mockMemTable).isFull();
    }

    @Test
    void testPutDuringRecovery() throws IOException {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        long ttl = 60;

        recovering.set(true);
        when(mockMemTable.put(key, value, ttl)).thenReturn(true);

        int result = putStore.put(key, value, ttl);

        assertEquals(PutStore.RESULT_SUCCESS, result);
        verifyNoInteractions(mockWal); // WAL should not be called during recovery
        verify(mockMemTable).put(key, value, ttl);
    }

    @Test
    void testUpdateDependencies() {
        // Create new mocks
        MemTable newMockMemTable = Mockito.mock(MemTable.class);
        ReadWriteLock newReadWriteLock = new ReentrantReadWriteLock();
        WAL newMockWal = Mockito.mock(WAL.class);
        AtomicBoolean newRecovering = new AtomicBoolean(true);

        // Update dependencies
        putStore.updateDependencies(
            newMockMemTable,
            newReadWriteLock,
            newMockWal,
            newRecovering
        );

        // Test that the new dependencies are used
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        long ttl = 60;

        when(newMockMemTable.put(key, value, ttl)).thenReturn(true);

        int result = putStore.put(key, value, ttl);

        assertEquals(PutStore.RESULT_SUCCESS, result);
        verifyNoInteractions(newMockWal); // WAL should not be called during recovery (newRecovering is true)
        verify(newMockMemTable).put(key, value, ttl);
    }
}
