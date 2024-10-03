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

import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.Objects;
import org.apache.hadoop.shaded.org.apache.avro.Schema;
import org.apache.hadoop.shaded.org.apache.avro.generic.IndexedRecord;

public class AddFile implements IndexedRecord {
  private String path;
  private final Schema avroSchema;

  static final int PATH = 20_600;

  public static final StructType DELTA_SCHEMA = new StructType().add("path", StringType.STRING);

  private static final Schema AVRO_SCHEMA = AvroUtil.convert(DELTA_SCHEMA, AddFile.class);

  // Used by Avro reflection to instantiate this class when reading events
  public AddFile(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public AddFile(String path) {
    requireNonNull(path, "path cannot be null");
    this.path = path;
    this.avroSchema = AVRO_SCHEMA;
  }

  public String getPath() {
    return path;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case PATH:
        this.path = v.toString();
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case PATH:
        return path;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AddFile that = (AddFile) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
