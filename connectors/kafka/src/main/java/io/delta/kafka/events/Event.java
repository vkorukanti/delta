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
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.shaded.org.apache.avro.Schema;
import org.apache.hadoop.shaded.org.apache.avro.generic.IndexedRecord;

/**
 * Class representing all events produced to the control topic. Different event types have different
 * payloads.
 */
public class Event implements IndexedRecord {

  private static final PayloadType[] PAYLOAD_TYPE_VALUES = PayloadType.values();
  private UUID id;
  private PayloadType type;
  private OffsetDateTime timestamp;
  private String groupId;
  private Payload payload;
  private final Schema avroSchema;

  static final int ID = 10_500;
  static final int TYPE = 10_501;
  static final int TIMESTAMP = 10_502;
  static final int GROUP_ID = 10_503;
  static final int PAYLOAD = 10_504;

  // Used by Avro reflection to instantiate this class when reading events
  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(String groupId, Payload payload) {
    requireNonNull(groupId, "Group ID cannot be null");
    requireNonNull(payload, "Payload cannot be null");

    this.id = UUID.randomUUID();
    this.type = payload.type();
    this.timestamp = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    this.groupId = groupId;
    this.payload = payload;

    StructType deltaSchema =
        new StructType()
            .add("id", StringType.STRING)
            .add("type", IntegerType.INTEGER)
            .add("timestamp", TimestampNTZType.TIMESTAMP_NTZ)
            .add("group_id", StringType.STRING)
            .add("payload", payload.writeSchema());

    Map<Integer, String> typeMap = Maps.newHashMap(AvroUtil.FIELD_ID_TO_CLASS);
    typeMap.put(PAYLOAD, payload.getClass().getName());

    this.avroSchema = AvroUtil.convert(deltaSchema, getClass(), typeMap);
  }

  public UUID id() {
    return id;
  }

  public PayloadType type() {
    return type;
  }

  public OffsetDateTime timestamp() {
    return timestamp;
  }

  public Payload payload() {
    return payload;
  }

  public String groupId() {
    return groupId;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case ID:
        this.id = (UUID) v;
        return;
      case TYPE:
        this.type = v == null ? null : PAYLOAD_TYPE_VALUES[(Integer) v];
        return;
      case TIMESTAMP:
        this.timestamp = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
        return;
      case GROUP_ID:
        this.groupId = v == null ? null : v.toString();
        return;
      case PAYLOAD:
        this.payload = (Payload) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case ID:
        return id;
      case TYPE:
        return type == null ? null : type.id();
      case TIMESTAMP:
        return timestamp == null ? null : DateTimeUtil.microsFromTimestamptz(timestamp);
      case GROUP_ID:
        return groupId;
      case PAYLOAD:
        return payload;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
