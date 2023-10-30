/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.client;

import java.io.ByteArrayInputStream;

/**
 * Represents a write request to write the given set of bytes to a given file.
 */
public interface FileWriteRequest {
    /**
     * Get the fully qualified path of the file to which to write the data.
     */
    String getPath();

    /**
     * Get the data of the to write to the file.
     * Responsibility of the caller to close the stream
     */
    ByteArrayInputStream getDataStream();
}
