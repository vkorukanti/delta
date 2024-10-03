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
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.hadoop.shaded.org.apache.avro.Schema;

/**
 * A control event payload for events sent by a coordinator that indicates it has completed a commit
 * cycle. Events with this payload are not consumed by the sink, they are informational and can be
 * used by consumers to trigger downstream processes.
 */
public class CommitComplete implements Payload {

  private UUID commitId;
  private OffsetDateTime validThroughTs;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_000;
  static final int VALID_THROUGH_TS = 10_001;

  private static final StructType DELTA_SCHEMA =
      new StructType()
          .add("commit_id", StringType.STRING)
          .add("valid_through_ts", TimestampType.TIMESTAMP);

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(DELTA_SCHEMA, CommitComplete.class);

  // Used by Avro reflection to instantiate this class when reading events
  public CommitComplete(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public CommitComplete(UUID commitId, OffsetDateTime validThroughTs) {
    requireNonNull(commitId, "Commit ID cannot be null");
    this.commitId = commitId;
    this.validThroughTs = validThroughTs;
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public PayloadType type() {
    return PayloadType.COMMIT_COMPLETE;
  }

  public UUID commitId() {
    return commitId;
  }

  /**
   * Valid-through timestamp, which is the min-of-max record timestamps across all workers for the
   * commit.
   */
  public OffsetDateTime validThroughTs() {
    return validThroughTs;
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
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        this.commitId = (UUID) v;
        return;
      case VALID_THROUGH_TS:
        this.validThroughTs = v == null ? null : DateTimeUtil.timestamptzFromMicros((Long) v);
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
      case VALID_THROUGH_TS:
        return validThroughTs == null ? null : DateTimeUtil.microsFromTimestamptz(validThroughTs);
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
