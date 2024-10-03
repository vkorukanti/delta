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
package io.delta.kafka.events;

import static java.util.Objects.requireNonNull;

import io.delta.kafka.util.DateTimeUtil;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.time.OffsetDateTime;
import org.apache.hadoop.shaded.org.apache.avro.Schema;
import org.apache.hadoop.shaded.org.apache.avro.generic.IndexedRecord;

/** Element representing an offset, with topic name, partition number, and offset. */
public class TopicPartitionOffset implements IndexedRecord {

  private String topic;
  private Integer partition;
  private Long offset;
  private OffsetDateTime timestamp;
  private final Schema avroSchema;

  static final int TOPIC = 10_700;
  static final int PARTITION = 10_701;
  static final int OFFSET = 10_702;
  static final int TIMESTAMP = 10_703;

  public static final StructType DELTA_SCHEMA =
      new StructType()
          .add("topic", StringType.STRING)
          .add("partition", IntegerType.INTEGER)
          .add("offset", LongType.LONG)
          .add("timestamp", TimestampType.TIMESTAMP);

  private static final Schema AVRO_SCHEMA =
      AvroUtil.convert(DELTA_SCHEMA, TopicPartitionOffset.class);

  // Used by Avro reflection to instantiate this class when reading events
  public TopicPartitionOffset(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public TopicPartitionOffset(String topic, int partition, Long offset, OffsetDateTime timestamp) {
    requireNonNull(topic, "Topic cannot be null");
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String topic() {
    return topic;
  }

  public Integer partition() {
    return partition;
  }

  public Long offset() {
    return offset;
  }

  public OffsetDateTime timestamp() {
    return timestamp;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TOPIC:
        this.topic = v == null ? null : v.toString();
        return;
      case PARTITION:
        this.partition = (Integer) v;
        return;
      case OFFSET:
        this.offset = (Long) v;
        return;
      case TIMESTAMP:
        this.timestamp = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case TOPIC:
        return topic;
      case PARTITION:
        return partition;
      case OFFSET:
        return offset;
      case TIMESTAMP:
        return timestamp == null ? null : DateTimeUtil.microsFromTimestamptz(timestamp);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
