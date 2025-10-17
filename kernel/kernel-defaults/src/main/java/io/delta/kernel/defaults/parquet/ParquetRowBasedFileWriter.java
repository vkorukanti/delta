/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.parquet;

import static io.delta.kernel.defaults.internal.parquet.ParquetIOUtils.createParquetOutputFile;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Meta;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.internal.parquet.ParquetIOUtils;
import io.delta.kernel.defaults.internal.parquet.ParquetRowWriters;
import io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * Parquet file writer that write data into Parquet files row by row using {@link Row} abstraction.
 *
 * <p>Usage example:
 *
 * <pre>
 *     ParquetRowBasedFileWriter writer = new ParquetRowBasedFileWriter(
 *        fileIO, outputFile, schema, true, Collections.emptyList());
 *
 *     try {
 *      for (Row row : rowsToWrite) {
 *        writer.write(row);
 *
 *        // Optionally, get the current length of the file being written
 *        // to decide when to roll over to a new file.
 *        long currentLength = writer.length();
 *      }
 *    } finally {
 *      writer.close();
 *    }
 *
 *   DataFileStatus fileStatus = writer.getFileStatus();
 * </pre>
 */
public class ParquetRowBasedFileWriter implements AutoCloseable {
  private final FileIO fileIO;
  private final OutputFile parquetFile;
  private final StructType schema;
  private final boolean atomicWrite;
  private final List<Column> statsColumns;
  private final ParquetRowWriters.ColumnWriter[] rowColumnWriters;

  // State
  private ParquetWriter<Row> parquetWriter;
  private boolean isClosed = false;
  private RecordConsumer recordConsumer;
  private long writtenRowCount = 0;

  /**
   * Create a Parquet row-based file writer.
   *
   * @param fileIO Instance of {@link FileIO} for IO operations.
   * @param parquetFile Parquet output file (location) to write to.
   * @param schema Schema of the data to write. All rows written must conform to this schema.
   * @param atomicWrite Whether to use atomic write for the Parquet file. When true, the file is
   *     written with all the data or no file is created at all.
   * @param statsColumns List of columns for which statistics need to be collected in the file
   *     metadata.
   */
  public ParquetRowBasedFileWriter(
      FileIO fileIO,
      OutputFile parquetFile,
      StructType schema,
      boolean atomicWrite,
      List<Column> statsColumns) {
    this.fileIO = requireNonNull(fileIO, "fileIO is null");
    this.parquetFile = requireNonNull(parquetFile, "parquetFile is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.atomicWrite = atomicWrite;
    this.statsColumns = requireNonNull(statsColumns, "statsColumns is null");
    this.rowColumnWriters = ParquetRowWriters.createColumnWriters(schema);
  }

  /**
   * Write a row to the Parquet file.
   *
   * @param row Row to write.
   * @throws IOException if an I/O error occurs.
   */
  public void write(Row row) throws IOException {
    checkArgument(!isClosed, "Cannot write to a closed ParquetRowFileWriter.");

    // Lazy initialization of Parquet writer
    ensureParquetWriterIsOpen();

    // Write the row
    parquetWriter.write(row);
    writtenRowCount++;
  }

  /** Get the current length of the Parquet file being written. */
  public long length() {
    return parquetWriter.getDataSize();
  }

  /**
   * Get the {@link DataFileStatus} for the Parquet file being written.
   *
   * <p>This method can be called only after closing the writer.
   *
   * @return DataFileStatus for the Parquet file.
   */
  public DataFileStatus getFileStatus() {
    checkArgument(
        !isClosed,
        "Current file is not yet closed." + "File status is available after closing of the file.");

    return ParquetIOUtils.constructDataFileStatus(
        fileIO, parquetFile.path(), schema, statsColumns, writtenRowCount);
  }

  /** Close the Parquet file writer. */
  @Override
  public void close() throws Exception {
    if (!isClosed) {
      isClosed = true;
      Utils.closeCloseables(parquetWriter);
    }
  }

  private void ensureParquetWriterIsOpen() throws IOException {
    ParquetIOUtils.createWriter(
        fileIO,
        createParquetOutputFile(parquetFile, atomicWrite),
        new RowWriteSupport(schema, ParquetSchemaUtils.toParquetSchema(schema), rowColumnWriters));
  }

  private static class RowWriteSupport extends WriteSupport<Row> {
    final StructType inputSchema;
    final MessageType parquetSchema;

    private final ParquetRowWriters.ColumnWriter[] rowColumnWriters;
    private RecordConsumer recordConsumer;

    RowWriteSupport(
        StructType inputSchema, // WriteSupport created for this specific schema
        MessageType parquetSchema,
        ParquetRowWriters.ColumnWriter[] rowColumnWriters) { // Parquet equivalent schema
      this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
      this.parquetSchema = requireNonNull(parquetSchema, "parquetSchema is null");
      this.rowColumnWriters = requireNonNull(rowColumnWriters, "rowColumnWriters is null");
    }

    @Override
    public String getName() {
      return "delta-kernel-default-parquet-row-based-writer";
    }

    @Override
    public WriteContext init(Configuration configuration) {
      Map<String, String> extraProps =
          Collections.singletonMap(
              "io.delta.kernel.default-parquet-writer", "Kernel-Defaults-" + Meta.KERNEL_VERSION);
      return new WriteContext(parquetSchema, extraProps);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Row row) {
      // Use java asserts which are disabled in prod to reduce the overhead
      // and enabled in tests with `-ea` argument.
      assert (recordConsumer != null) : "Parquet record consumer is null";
      recordConsumer.startMessage();
      for (int i = 0; i < rowColumnWriters.length; i++) {
        rowColumnWriters[i].writeColumnValue(recordConsumer, row);
      }
      recordConsumer.endMessage();
    }
  }
}
