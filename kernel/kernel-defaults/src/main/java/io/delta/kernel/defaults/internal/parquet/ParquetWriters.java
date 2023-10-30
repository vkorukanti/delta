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
package io.delta.kernel.defaults.internal.parquet;

import static java.util.Objects.requireNonNull;

import org.apache.parquet.io.api.RecordConsumer;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;

class ParquetWriters {
    private ParquetWriters() {
    }

    static ColumnVectorWriter[] createColumnVectorWriters(ColumnarBatch batch) {
        requireNonNull(batch, "batch is null");

        int numCols = batch.getSchema().length();

        ColumnVectorWriter[] columnVectorWriters = new ColumnVectorWriter[numCols];
        for (int columnOrdinal = 0; columnOrdinal < numCols; columnOrdinal++) {
            ColumnVector columnVector = batch.getColumnVector(columnOrdinal);
            columnVectorWriters[columnOrdinal] = createColumnVectorWriter(columnVector);
        }
        return columnVectorWriters;
    }

    private static ColumnVectorWriter createColumnVectorWriter(ColumnVector columnVector) {
        DataType dataType = columnVector.getDataType();

        if (dataType instanceof IntegerType) {
            return new IntVectorWriter(columnVector);
        }

        throw new IllegalArgumentException("Unsupported column vector type: " + dataType);
    }

    interface ColumnVectorWriter {
        void writeRowValue(int fieldId, RecordConsumer recordConsumer, int rowId);
    }

    static class IntVectorWriter implements ColumnVectorWriter {
        private final ColumnVector intVector;

        IntVectorWriter(ColumnVector intVector) {
            this.intVector = intVector;
        }

        public void writeRowValue(int fieldId, RecordConsumer recordConsumer, int rowId) {
            if (!intVector.isNullAt(rowId)) {
                recordConsumer.startField("field1", fieldId);
                recordConsumer.addInteger(intVector.getInt(rowId));
                recordConsumer.endField("field1", fieldId);
            }
        }
    }
}
