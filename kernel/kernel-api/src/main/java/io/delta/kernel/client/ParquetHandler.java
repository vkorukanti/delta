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

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

/**
 * Provides Parquet file related functionalities to Delta Kernel. Connectors can leverage this
 * interface to provide their own custom implementation of Parquet data file functionalities to
 * Delta Kernel.
 *
 * @since 3.0.0
 */
@Evolving
public interface ParquetHandler {
    /**
     * Read the Parquet format files at the given locations and return the data as a
     * {@link ColumnarBatch} with the columns requested by {@code physicalSchema}.
     * <p>
     * If {@code physicalSchema} has a {@link StructField} with column name
     * {@link StructField#METADATA_ROW_INDEX_COLUMN_NAME} and the field is a metadata column
     * {@link StructField#isMetadataColumn()} the column must be populated with the file row index.
     * <p>
     * How does a column in {@code physicalSchema} match to the column in the Parquet file?
     * If the {@link StructField} has a field id in the {@code metadata} with key `parquet.field.id`
     * the column is attempted to match by id. If the column is not found by id, the column is
     * matched by name. When trying to find the column in Parquet by name,
     * first case-sensitive match is used. If not found then a case-insensitive match is attempted.
     *
     * @param fileIter       Iterator of files to read data from.
     * @param physicalSchema Select list of columns to read from the Parquet file.
     * @param predicate      Optional predicate which the Parquet reader can optionally use to prune
     *                       rows that don't satisfy the predicate. Because pruning is optional and
     *                       may be incomplete, caller is still responsible apply the predicate on
     *                       the data returned by this method.
     * @return an iterator of {@link ColumnarBatch}s containing the data in columnar format.
     * It is the responsibility of the caller to close the iterator. The data returned is in the
     * same as the order of files given in {@code scanFileIter}.
     * @throws IOException if an I/O error occurs during the read.
     */
    CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> fileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) throws IOException;

    /**
     * Write the given data batches to a Parquet files. Try to keep the Parquet
     * file size to given size. If the current file exceeds this size,
     * create a new Parquet file.
     * <p>
     *
     * @param directoryPath     Path to the directory where the Parquet files will be written.
     * @param dataIter          Iterator of data batches to write.
     * @param maxFileSize      Maximum size of the created Parquet file in bytes.
     * @param statisticsSchema Schema of the columns to collect statistics for. This is a subset of
     *                         the schema of the data that is being written. The schema is used to
     *                         to collect statistics for each Parquet file.
     * @return an iterator of {@link Row}s containing the metadata of the written Parquet files.
     * For each written Parquet file a row is returned that contains the following schema
     *
     * (
     *   path: String,
     *   size: Long,
     *   modificationTime: Long,
     *   stats: StructType
     *      - numRecords: Long // number of records in the file
     *      // Min values of the columns in the file. THe columns to collect stats is defined
     *      // by the `statisticsSchema` parameter. Only the leaf-level columns have the stats.
     *      - minValues: StructType
     *      // Max values of the columns in the file. THe columns to collect stats is defined
     *      // by the `statisticsSchema` parameter. Only the leaf-level columns have the stats.
     *      - maxValues: StructType
     *      // Null count of columns in the file. THe columns to collect stats is defined
     *      // by the `statisticsSchema` parameter. Only the leaf-level columns have the stats.
     *      - nullCount: StructType
     * )
     */
    CloseableIterator<Row> writeParquetFiles(
            String directoryPath,
            CloseableIterator<FilteredColumnarBatch> dataIter,
            long maxFileSize,
            StructType statisticsSchema) throws IOException;
}
