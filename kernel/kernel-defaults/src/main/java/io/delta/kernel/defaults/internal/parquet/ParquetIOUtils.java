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

import static io.delta.kernel.defaults.internal.parquet.ParquetStatsReader.readDataFileStatistics;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.COMPRESSION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.DICTIONARY_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.MAX_PADDING_BYTES;
import static org.apache.parquet.hadoop.ParquetOutputFormat.PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetOutputFormat.VALIDATION;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.PositionOutputStream;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * Utilities related to Parquet I/O. These utilities bridge the gap between Kernel's {@link
 * io.delta.kernel.defaults.engine.fileio.FileIO} and the Parquet I/O classes.
 */
public class ParquetIOUtils {
  private ParquetIOUtils() {}

  /**
   * Helper method to create {@link ParquetWriter} for given file path and write support. It makes
   * use of configuration options in `configuration` to configure the writer. Different available
   * configuration options are defined in {@link ParquetOutputFormat}.
   */
  public static <T> ParquetWriter<T> createWriter(
      FileIO fileIO, org.apache.parquet.io.OutputFile outputFile, WriteSupport<T> writeSupport)
      throws IOException {
    ParquetRowDataBuilder<T> rowDataBuilder = new ParquetRowDataBuilder<>(outputFile, writeSupport);

    fileIO
        .getConf(COMPRESSION)
        .ifPresent(
            compression ->
                rowDataBuilder.withCompressionCodec(CompressionCodecName.fromConf(compression)));

    fileIO.getConf(BLOCK_SIZE).map(Long::parseLong).ifPresent(rowDataBuilder::withRowGroupSize);

    fileIO.getConf(PAGE_SIZE).map(Integer::parseInt).ifPresent(rowDataBuilder::withPageSize);

    fileIO
        .getConf(DICTIONARY_PAGE_SIZE)
        .map(Integer::parseInt)
        .ifPresent(rowDataBuilder::withDictionaryPageSize);

    fileIO
        .getConf(MAX_PADDING_BYTES)
        .map(Integer::parseInt)
        .ifPresent(rowDataBuilder::withMaxPaddingSize);

    fileIO
        .getConf(ENABLE_DICTIONARY)
        .map(Boolean::parseBoolean)
        .ifPresent(rowDataBuilder::withDictionaryEncoding);

    fileIO.getConf(VALIDATION).map(Boolean::parseBoolean).ifPresent(rowDataBuilder::withValidation);

    fileIO
        .getConf(WRITER_VERSION)
        .map(ParquetProperties.WriterVersion::fromString)
        .ifPresent(rowDataBuilder::withWriterVersion);

    return rowDataBuilder.build();
  }

  /** Create a Parquet {@link org.apache.parquet.io.InputFile} from a Kernel's {@link InputFile}. */
  static org.apache.parquet.io.InputFile createParquetInputFile(InputFile inputFile) {
    return new org.apache.parquet.io.InputFile() {
      @Override
      public long getLength() throws IOException {
        return inputFile.length();
      }

      @Override
      public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
        SeekableInputStream seekableStream = inputFile.newStream();
        return new DelegatingSeekableInputStream(seekableStream) {
          @Override
          public void seek(long newPos) throws IOException {
            seekableStream.seek(newPos);
          }

          @Override
          public long getPos() throws IOException {
            return seekableStream.getPos();
          }
        };
      }
    };
  }

  /**
   * Create a Parquet {@link org.apache.parquet.io.OutputFile} from a Kernel's {@link OutputFile}.
   */
  public static org.apache.parquet.io.OutputFile createParquetOutputFile(
      OutputFile kernelOutputFile, boolean atomicWrite) {
    return new org.apache.parquet.io.OutputFile() {
      @Override
      public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint)
          throws IOException {
        // blockSizeHint is hint used in HDFS compliant file systems. In cloud storage systems
        // it is irrelevant. So, we ignore it.
        PositionOutputStream posOutputStream = kernelOutputFile.create(atomicWrite);
        return new DelegatingPositionOutputStream(posOutputStream) {
          @Override
          public long getPos() throws IOException {
            return posOutputStream.getPos();
          }
        };
      }

      @Override
      public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint)
          throws IOException {
        // In Kernel we never overwrite files, so this method is not used.
        throw new UnsupportedOperationException("createOrOverwrite is not supported in Kernel");
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        // blockSizeHint is hint used in HDFS compliant file systems. In cloud storage systems
        // it is irrelevant. So, return some default value.
        return 128 * 1024 * 1024; // 128MB
      }

      @Override
      public String getPath() {
        return kernelOutputFile.path();
      }
    };
  }

  /**
   * Construct the {@link DataFileStatus} for the given file path. It reads the file status and
   * Parquet footer to compute the statistics for the file.
   *
   * <p>Potential improvement in future to directly compute the statistics while writing the file if
   * this becomes a sufficiently large part of the write operation time.
   *
   * @param fileIO the FileIO implementation to use to read the file
   * @param path the path of the file
   * @param dataSchema the schema of the data in the file
   * @param statsColumns the columns for which stats are required
   * @param numRows the number of rows in the file. If no column stats are required, this is used to
   *     construct the {@link DataFileStatistics}. Otherwise, the stats are read from the file.
   * @return the {@link DataFileStatus} for the file
   */
  public static DataFileStatus constructDataFileStatus(
      FileIO fileIO, String path, StructType dataSchema, List<Column> statsColumns, long numRows) {
    try {
      // Get the FileStatus to figure out the file size and modification time
      FileStatus fileStatus = fileIO.getFileStatus(path);
      String resolvedPath = fileIO.resolvePath(path);

      DataFileStatistics stats;
      if (statsColumns.isEmpty()) {
        stats =
            new DataFileStatistics(
                numRows,
                emptyMap() /* minValues */,
                emptyMap() /* maxValues */,
                emptyMap() /* nullCount */,
                Optional.empty() /* tightBounds */);
      } else {
        stats =
            readDataFileStatistics(
                fileIO.newInputFile(resolvedPath, fileStatus.getSize()), dataSchema, statsColumns);
      }

      return new DataFileStatus(
          resolvedPath, fileStatus.getSize(), fileStatus.getModificationTime(), Optional.of(stats));
    } catch (IOException ioe) {
      throw new UncheckedIOException("Failed to read the stats for: " + path, ioe);
    }
  }

  private static class ParquetRowDataBuilder<T>
      extends ParquetWriter.Builder<T, ParquetRowDataBuilder<T>> {
    private final WriteSupport<T> writeSupport;

    protected ParquetRowDataBuilder(
        org.apache.parquet.io.OutputFile outputFile, WriteSupport<T> writeSupport) {
      super(outputFile);
      this.writeSupport = requireNonNull(writeSupport, "writeSupport is null");
    }

    @Override
    protected ParquetRowDataBuilder<T> self() {
      return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
      return writeSupport;
    }
  }
}
