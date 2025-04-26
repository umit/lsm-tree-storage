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
 * Record for a delete operation in the WAL.
 * This record contains a key to be deleted.
 */
public class DeleteRecord implements Record {
    private final long sequenceNumber;
    private final byte[] key;

    /**
     * Creates a new DeleteRecord.
     *
     * @param sequenceNumber the sequence number of this record
     * @param key            the key as byte array
     */
    public DeleteRecord(long sequenceNumber, byte[] key) {
        this.sequenceNumber = sequenceNumber;
        this.key = key.clone(); // Defensive copy
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public byte[] getKey() {
        return key.clone(); // Return a copy to prevent modification
    }

    @Override
    public String toString() {
        return "DeleteRecord{" +
                "sequenceNumber=" + sequenceNumber +
                ", key=" + Arrays.toString(key) +
                '}';
    }
}