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

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.shaded.org.apache.avro.Schema;

/**
 * A control event payload for events sent by a worker that indicates it has finished sending all
 * data for a commit request.
 */
public class DataComplete implements Payload {

  private UUID commitId;
  private List<TopicPartitionOffset> assignments;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_100;
  static final int ASSIGNMENTS = 10_101;
  static final int ASSIGNMENTS_ELEMENT = 10_102;

  private static final StructType DELTA_SCHEMA =
      new StructType()
          .add("commit_id", StringType.STRING)
          .add("assignments", new ArrayType(TopicPartitionOffset.DELTA_SCHEMA, true));

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(DELTA_SCHEMA, DataComplete.class);

  // Used by Avro reflection to instantiate this class when reading events
  public DataComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataComplete(UUID commitId, List<TopicPartitionOffset> assignments) {
    requireNonNull(commitId, "Commit ID cannot be null");
    this.commitId = commitId;
    this.assignments = assignments;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_COMPLETE;
  }

  public UUID commitId() {
    return commitId;
  }

  public List<TopicPartitionOffset> assignments() {
    return assignments;
  }

  @Override
  public StructType writeSchema() {
    return DELTA_SCHEMA;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        this.commitId = (UUID) v;
        return;
      case ASSIGNMENTS:
        this.assignments = (List<TopicPartitionOffset>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        return commitId;
      case ASSIGNMENTS:
        return assignments;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
