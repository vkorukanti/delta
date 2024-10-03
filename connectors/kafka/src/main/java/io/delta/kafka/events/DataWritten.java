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
 * A control event payload for events sent by a worker that contains the table data that has been
 * written and is ready to commit.
 */
public class DataWritten implements Payload {

  private StructType partitionType;

  private UUID commitId;
  private List<String> dataFiles;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_300;
  static final int TABLE_REFERENCE = 10_301;
  static final int DATA_FILES = 10_302;
  static final int DATA_FILES_ELEMENT = 10_303;

  public static final StructType DELTA_SCHEMA =
      new StructType()
          .add("commit_id", StringType.STRING)
          .add("data_files", new ArrayType(StringType.STRING, false));

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(DELTA_SCHEMA, DataWritten.class);

  // Used by Avro reflection to instantiate this class when reading events, note that this does not
  // set the partition type so the instance cannot be re-serialized
  public DataWritten(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataWritten(UUID commitId, List<String> dataFiles) {
    requireNonNull(commitId, "Commit ID cannot be null");
    this.commitId = commitId;
    this.dataFiles = dataFiles;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_WRITTEN;
  }

  public UUID commitId() {
    return commitId;
  }

  public List<String> dataFiles() {
    return dataFiles;
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
      case DATA_FILES:
        this.dataFiles = (List<String>) v;
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
      case DATA_FILES:
        return dataFiles;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
