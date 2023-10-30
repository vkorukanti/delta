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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.util.*;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.shaded.com.google.common.collect.*;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import static org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.hadoop.shaded.com.google.common.collect.Iterables.getOnlyElement;

import io.delta.kernel.data.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.data.GenericRow;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.parquet.ParquetWriters.ColumnVectorWriter;

public class ParquetBatchWriter {
    private final Configuration configuration;
    private final Path directory;
    private final long maxFileSize;
    private final StructType statsSchema;

    // state
    private long currentFileNumber;

    // TODO: need to design how to share the memory for multiple writers within the same task.
    private MemoryManager memoryManager;

    public ParquetBatchWriter(
            Configuration configuration,
            Path directory,
            long maxFileSize,
            StructType statsSchema) {
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.directory = requireNonNull(directory, "directory is null");
        checkArgument(maxFileSize > 0, "invalid max Parquet file size: " + maxFileSize);
        this.maxFileSize = maxFileSize;
        this.statsSchema = requireNonNull(statsSchema, "statsSchema is null");
    }

    public CloseableIterator<Row> write(CloseableIterator<FilteredColumnarBatch> data) {
        return new CloseableIterator<Row>() {
            // current state
            private Optional<Row> nextResult = Optional.empty();
            private Optional<FilteredColumnarBatch> currentBatch = Optional.empty();
            private Optional<Integer> currentBatchRowId = Optional.empty();

            @Override
            public void close() throws IOException {
                // TODO
            }

            @Override
            public boolean hasNext() {
                if (nextResult.isPresent()) {
                    return true;
                }
                try {
                    nextResult = writeNextFile();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return nextResult.isPresent();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Row toReturn = nextResult.get();
                nextResult = Optional.empty();
                return toReturn;
            }


            private Optional<Row> writeNextFile()
                throws IOException, InterruptedException {
                if (!currentBatch.isPresent() ||
                    currentBatch.get().getData().getSize() >= currentBatchRowId.get()) {
                    if (!data.hasNext()) {
                        return Optional.empty();
                    }
                    currentBatch = Optional.of(data.next());
                    currentBatchRowId = Optional.empty();
                }

                FilteredColumnarBatch batch = currentBatch.get();
                ColumnarBatch data = batch.getData();
                MessageType parquetSchema = ParquetSchemaUtils.toParquetSchema(data.getSchema());

                ColumnVectorWriter[] columnVectorWriters =
                    ParquetWriters.createColumnVectorWriters(data);

                BatchWriteSupport writeSupport =
                    new BatchWriteSupport(parquetSchema, emptyMap());

                // If we move to the next columnar batch, just need to set the new column writers
                writeSupport.setColumnVectorWriters(columnVectorWriters);

                Path filePath = generateNextFilePath();
                ParquetRecordWriter<Integer> writer = null;
                try {
                    writer = createWriter(filePath, parquetSchema, writeSupport);
                    int currentRowId = currentBatchRowId.orElse(0);
                    for (; currentRowId < data.getSize(); currentRowId++) {
                        if (!batch.getSelectionVector().isPresent() ||
                            batch.getSelectionVector().get().getBoolean(currentRowId)) {
                            writer.write(null, currentRowId);
                        }
                    }
                    currentBatchRowId = Optional.of(currentRowId);
                } catch (Exception e) {
                    throw new IOException("Failed to write batch", e);
                } finally {
                    if (writer != null) {
                        writer.close(null);
                    }
                }

                return Optional.of(constructOutputRow(filePath.toString()));
            }
        };
    }

    private static class BatchWriteSupport extends WriteSupport<Integer> {
        private final MessageType parquetSchema;
        private final Map<String, String> extraProps;

        private ColumnVectorWriter[] columnVectorWriters;
        private RecordConsumer recordConsumer;

        BatchWriteSupport(MessageType parquetSchema,
                                 Map<String, String> extraProps) {
            this.parquetSchema = requireNonNull(parquetSchema, "parquetSchema is null");
            this.extraProps = requireNonNull(extraProps, "extraProps is null");
        }

        void setColumnVectorWriters(ColumnVectorWriter[] columnVectorWriters) {
            this.columnVectorWriters =
                requireNonNull(columnVectorWriters, "columnVectorWriters is null");
        }

        @Override
        public String getName() {
            return "delta-kernel-default-parquet-writer";
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(parquetSchema, extraProps);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Integer rowId) {
            assert (recordConsumer != null) : "Parquet record consumer is null";
            assert (columnVectorWriters != null) : "Column writers are not set";
            recordConsumer.startMessage();
            for (int i = 0; i < columnVectorWriters.length; i++) {
                columnVectorWriters[i].writeRowValue(i, recordConsumer, rowId);
            }
            recordConsumer.endMessage();
        }
    }

    private Path generateNextFilePath() {
        return new Path(directory, UUID.randomUUID() + "-" + currentFileNumber++ + ".parquet");
    }

    private ParquetRecordWriter<Integer> createWriter(
        Path filePath,
        MessageType parquetSchema,
        WriteSupport<Integer> writeSupport) throws IOException {
        ParquetFileWriter w = new ParquetFileWriter(
            HadoopOutputFile.fromPath(filePath, configuration),
            parquetSchema,
            ParquetFileWriter.Mode.CREATE,
            maxFileSize, // Make the max file size as the block size
            ParquetWriter.MAX_PADDING_SIZE_DEFAULT
        );
        w.start();

        float maxLoad = 0.95f;
        long minAllocation = 1024 * 1024; // 1MB
        if (memoryManager == null) {
            memoryManager = new MemoryManager(maxLoad, minAllocation);
        }

        try {
            Constructor<ParquetRecordWriter> constructor = (Constructor<ParquetRecordWriter>)
                Arrays.stream(ParquetRecordWriter.class.getDeclaredConstructors())
                    .filter(
                        c -> c.getParameterCount() == 10 &&
                            c.getParameterTypes()[5].equals(CompressionCodecName.class))
                    .findFirst().orElseThrow(
                        () -> new RuntimeException("failed to find the constructor"));

            constructor.setAccessible(true);

            return (ParquetRecordWriter<Integer>) constructor.newInstance(
                w,
                writeSupport,
                parquetSchema,
                emptyMap(),
                maxFileSize,
                CompressionCodecName.SNAPPY,
                false, /* validating */
                ParquetProperties.builder().build(), // TODO: going with the default props
                memoryManager,
                configuration);
        } catch (Exception e) {
            throw new RuntimeException("failed to create the ParquetRecordWriter", e);
        }
    }

    private Row constructOutputRow(String path) {
        try {
            // Get the FileStatus to figure out the file size and modification time
            Path hadoopPath = new Path(path);
            FileSystem hadoopFs = hadoopPath.getFileSystem(configuration);
            FileStatus fileStatus = hadoopFs.getFileStatus(hadoopPath);

            // Read the Parquet footer to compute the statistics
            ParquetMetadata footer = ParquetFileReader.readFooter(configuration, hadoopPath);
            ImmutableMultimap.Builder<String, ColumnChunkMetaData> metadataForColumn =
                    ImmutableMultimap.builder();
            long rowCount = 0;
            for (BlockMetaData blockMetaData : footer.getBlocks()) {
                rowCount += blockMetaData.getRowCount();
                for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                    if (columnChunkMetaData.getPath().size() != 1) {
                        continue; // Only base column stats are supported
                    }
                    String columnName = getOnlyElement(columnChunkMetaData.getPath());
                    metadataForColumn.put(columnName, columnChunkMetaData);
                }
            }

            Row statsRow = mergeStats(metadataForColumn.build(), statsSchema, rowCount);

            Map<Integer, Object> valueMap = new HashMap<>();
            valueMap.put(0, path); // path
            valueMap.put(1, fileStatus.getLen()); // size
            valueMap.put(2, fileStatus.getModificationTime()); // modificationTime
            valueMap.put(3, statsRow); // statistics

            return new GenericRow(getOutputSchema(statsSchema), valueMap);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    static StructType getOutputSchema(StructType statsSchema) {
        return new StructType()
                .add("path", StringType.STRING, false /* nullable */)
                .add("size", LongType.LONG, false /* nullable */)
                .add("modificationTime", LongType.LONG, false /* nullable */)
                .add("statistics", getStatsSchema(statsSchema));
    }

    private static StructType getStatsSchema(StructType statsSchema) {
        return new StructType()
                .add("numRecords", LongType.LONG)
                .add("minValues", statsSchema)
                .add("maxValues", statsSchema)
                .add("nullCount", nullCountSchema(statsSchema));
    }

    private static StructType nullCountSchema(StructType statsSchema) {
        StructType nullCountSchema = new StructType();
        for (StructField field : statsSchema.fields()) {
            if (field.getDataType() instanceof StructType) {
                nullCountSchema.add(
                        field.getName(),
                        nullCountSchema((StructType) field.getDataType()));
            } else {
                nullCountSchema.add(field.getName(), LongType.LONG);
            }
        }
        return nullCountSchema;
    }

    private static Row mergeStats(
            Multimap<String, ColumnChunkMetaData> metadataForColumn,
            StructType statsSchema,
            long rowCount) {
        Map<String, Optional<Statistics<?>>> statsForColumn = metadataForColumn.keySet().stream()
                .collect(toImmutableMap(
                        identity(),
                        key -> mergeMetadataList(metadataForColumn.get(key))));

        Map<Integer, Object> minValues = new HashMap<>();
        Map<Integer, Object> maxValues = new HashMap<>();
        Map<Integer, Object> nullCounts = new HashMap<>();

        int fieldIndex = 0;
        for (StructField field : statsSchema.fields()) {
            String columnName = field.getName();
            Optional<Statistics<?>> stats = statsForColumn.get(columnName);
            if (stats.isPresent() &&
                    // Support stats on struct type children
                    !(field.getDataType() instanceof StructType)) {
                Statistics<?> statistics = stats.get();
                minValues.put(
                        fieldIndex,
                        decodeStatValue(statistics.genericGetMin(), field.getDataType()));
                maxValues.put(
                        fieldIndex,
                        decodeStatValue(statistics.genericGetMin(), field.getDataType()));
                nullCounts.put(fieldIndex,
                        statistics.isNumNullsSet() ? statistics.getNumNulls() : null);
            } else {
                minValues.put(fieldIndex, null);
                maxValues.put(fieldIndex, null);
                nullCounts.put(fieldIndex, null);
            }
        }

        return new GenericRow(
                getStatsSchema(statsSchema),
                ImmutableMap.of(
                        0, rowCount,
                        1, new GenericRow(statsSchema, minValues),
                        2, new GenericRow(statsSchema, maxValues),
                        3, new GenericRow(nullCountSchema(statsSchema), nullCounts)));
    }

    private static Object decodeStatValue(
            Object statValue,
            DataType dataType) {
        if (statValue == null) {
            return null;
        }

        if (dataType instanceof BooleanType) {
            return null; // there are no stats for boolean columns
        } else if (dataType instanceof ByteType) {
            return ((Number) statValue).byteValue();
        } else if (dataType instanceof ShortType) {
            return ((Number) statValue).shortValue();
        } else if (dataType instanceof IntegerType) {
            return ((Number) statValue).intValue();
        } else if (dataType instanceof LongType) {
            return ((Number) statValue).longValue();
        } else if (dataType instanceof FloatType) {
            return ((Number) statValue).floatValue();
        } else if (dataType instanceof DoubleType) {
            return ((Number) statValue).doubleValue();
        } else if (dataType instanceof StringType) {
            return statValue.toString();
        //} else if (dataType instanceof BinaryType) {
        //    return ((Binary) statValue).getBytes();
        //} else if (dataType instanceof DecimalType) {
        //    return ((Decimal) statValue).toBigDecimal();
        //} else if (dataType instanceof DateType) {
        //    return ((Date) statValue).toLocalDate();
        //} else if (dataType instanceof TimestampType) {
        //    return ((Timestamp) statValue).toLocalDateTime();
        //} else if (dataType instanceof StructType) {
        //    return statValue;
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private static Optional<Statistics<?>> mergeMetadataList(
            Collection<ColumnChunkMetaData> metadataList) {
        if (hasInvalidStatistics(metadataList)) {
            return Optional.empty();
        }

        return metadataList.stream()
                .<Statistics<?>>map(ColumnChunkMetaData::getStatistics)
                .reduce((statsA, statsB) -> {
                    statsA.mergeStatistics(statsB);
                    return statsA;
                });
    }

    public static boolean hasInvalidStatistics(Collection<ColumnChunkMetaData> metadataList) {
        return metadataList.stream().anyMatch(metadata ->
                // If any row group does not have stats collected,
                // stats for the file will not be valid
                !metadata.getStatistics().isNumNullsSet() ||
                        metadata.getStatistics().isEmpty() ||
                        // Columns with NaN values are marked by
                        // `hasNonNullValue` = false by the Parquet reader.
                        // See issue: https://issues.apache.org/jira/browse/PARQUET-1246
                        (!metadata.getStatistics().hasNonNullValue() &&
                                metadata.getStatistics().getNumNulls() !=
                                        metadata.getValueCount()));
    }
}
