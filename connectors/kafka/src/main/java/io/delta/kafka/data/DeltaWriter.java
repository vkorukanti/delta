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
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

class DeltaWriter implements RecordWriter {
  private final StructType tableSchema;
  private final DeltaSinkConfig config;
  private final List<DeltaWriterResult> writerResults;

  private RecordConverter recordConverter;

  DeltaWriter(DeltaSinkConfig config) {
    // TODO: get the schema from the config
    this.config = config;
    this.tableSchema = new StructType();
    this.writerResults = new ArrayList<>();
    initNewWriter();
  }

  private void initNewWriter() {
    // TODO: writer - Parquet
    this.recordConverter = new RecordConverter(tableSchema, config);
  }

  @Override
  public void write(SinkRecord record) {
    try {
      // ignore tombstones...
      if (record.value() != null) {
        Row row = convertToRow(record);
        // TODO: Call Parquet writer to write the data.
        // writer.write(row);
      }
    } catch (Exception e) {
      throw new DataException(
          String.format(
              "An error occurred converting record, topic: %s, partition, %d, offset: %d",
              record.topic(), record.kafkaPartition(), record.kafkaOffset()),
          e);
    }
  }

  private Row convertToRow(SinkRecord record) {
    return recordConverter.convert(record.value());
    // TODO: handle the schema evolution
  }

  private void flush() {
    // TODO: get the list of files written by the Parquet writer
    List<String> dataFiles = new ArrayList<>();

    writerResults.add(new DeltaWriterResult(dataFiles));
  }

  @Override
  public List<DeltaWriterResult> complete() {
    flush();

    List<DeltaWriterResult> result = Lists.newArrayList(writerResults);
    writerResults.clear();

    return result;
  }

  @Override
  public void close() {
    // try {
    // TODO: close the parquet writer writer.close();
    // } catch (IOException e) {
    //  throw new UncheckedIOException(e);
    // }
  }
}
