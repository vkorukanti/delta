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
package io.delta.kernel.internal.actions;

import io.delta.kernel.types.*;

public class CommitInfo {
    public static StructType READ_SCHEMA = new StructType()
            .add("timestamp", LongType.LONG)
            .add("engineInfo", StringType.STRING)
            .add("operation", StringType.STRING);

    private final String timestamp;
    private final String engineInfo;
    private final String operation;

    public CommitInfo(String timestamp, String engineInfo, String operation) {
        this.timestamp = timestamp;
        this.engineInfo = engineInfo;
        this.operation = operation;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getEngineInfo() {
        return engineInfo;
    }

    public String getOperation() {
        return operation;
    }
}
