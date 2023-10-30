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
package io.delta.kernel;

import java.util.List;
import java.util.Map;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

/**
 * Contains the context for writing data related to a partition to Delta table.
 */
public class WriteContext {
    private final String targetDirectory;
    private final long targetFileSizeInBytes;
    private final Map<String, Literal> partitionValues;
    private final List<Column> statsColumns;

    public WriteContext(
            String partitionPath,
            long targetFileSizeInBytes,
            Map<String, Literal> partitionValues,
            List<Column> statsColumns) {
        this.targetDirectory = partitionPath;
        this.targetFileSizeInBytes = targetFileSizeInBytes;
        this.partitionValues = partitionValues;
        this.statsColumns = statsColumns;
    }

    public String getTargetDirectory() {
        return targetDirectory;
    }

    public long getTargetFileSizeInBytes() {
        return targetFileSizeInBytes;
    }

    public Map<String, Literal> getPartitionValues() {
        return partitionValues;
    }

    public List<Column> getStatisticsSchema() {
        return statsColumns;
    }
}
