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

import java.util.Iterator;
import java.util.Map;

/**
 * An iterator over key-value pairs in a storage.
 * This interface extends the standard Java Iterator interface to provide
 * additional functionality specific to key-value storage.
 */
public interface KeyValueIterator extends Iterator<Map.Entry<byte[], byte[]>>, AutoCloseable {

    /**
     * Returns the key of the next entry without advancing the iterator.
     *
     * @return the key of the next entry, or null if there are no more entries
     */
    byte[] peekNextKey();

    /**
     * Closes this iterator, releasing any resources.
     * This method should be called when the iterator is no longer needed.
     */
    @Override
    void close();
}