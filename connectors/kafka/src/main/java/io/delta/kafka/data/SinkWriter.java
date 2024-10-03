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
package io.delta.kafka.data;

import io.delta.kafka.DeltaSinkConfig;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkWriter {
  private final DeltaSinkConfig config;
  private final DeltaWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;

  public SinkWriter(DeltaSinkConfig config) {
    this.config = config;
    this.writerFactory = new DeltaWriterFactory(config);
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
  }

  public void close() {
    writers.values().forEach(RecordWriter::close);
  }

  public SinkWriterResult completeWrite() {
    List<DeltaWriterResult> writerResults =
        writers.values().stream()
            .flatMap(writer -> writer.complete().stream())
            .collect(Collectors.toList());
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();

    return new SinkWriterResult(writerResults, offsets);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    sinkRecords.forEach(this::save);
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, timestamp));

    // if (config.dynamicTablesEnabled()) {
    //   routeRecordDynamically(record);
    // } else {
    //   routeRecordStatically(record);
    // }
    routeRecordStatically(record);
  }

  private void routeRecordStatically(SinkRecord record) {
    String tableName = "temp"; // TODO: fix this
    writerForTable(tableName, record, false).write(record);
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
