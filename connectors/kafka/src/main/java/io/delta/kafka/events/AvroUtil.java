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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.shaded.org.apache.avro.Schema;
import org.apache.hadoop.shaded.org.apache.avro.generic.IndexedRecord;

/** Class for Avro-related utility methods. */
public class AvroUtil {
  static final Map<Integer, String> FIELD_ID_TO_CLASS =
      ImmutableMap.of(
          DataComplete.ASSIGNMENTS_ELEMENT,
          TopicPartitionOffset.class.getName(),
          DataWritten.TABLE_REFERENCE,
          TableReference.class.getName(),
          DataWritten.DATA_FILES_ELEMENT,
          "org.apache.iceberg.GenericDataFile",
          CommitToTable.TABLE_NAME,
          TableReference.class.getName());

  public static byte[] encode(Event event) {
    throw new UnsupportedOperationException("NYI");
    // try {
    // return AvroEncoderUtil.encode(event, event.getSchema());
    // } catch (IOException e) {
    //  throw new UncheckedIOException(e);
    // }
  }

  public static Event decode(byte[] bytes) {
    throw new UnsupportedOperationException("NYI");
    //    try {
    //      Event event = AvroEncoderUtil.decode(bytes);
    //      // clear the cache to avoid memory leak
    //      DecoderResolver.clearCache();
    //      return event;
    //    } catch (IOException e) {
    //      throw new UncheckedIOException(e);
    //    }
  }

  static Schema convert(StructType deltaSchema, Class<? extends IndexedRecord> javaClass) {
    return convert(deltaSchema, javaClass, FIELD_ID_TO_CLASS);
  }

  static Schema convert(
      StructType deltaSchema,
      Class<? extends IndexedRecord> javaClass,
      Map<Integer, String> typeMap) {
    throw new UnsupportedOperationException("NYI");
    //    return AvroSchemaUtil.convert(
    //            deltaSchema,
    //        (fieldId, struct) ->
    //            struct.equals(icebergSchema) ? javaClass.getName() : typeMap.get(fieldId));
  }

  static int positionToId(int position, Schema avroSchema) {
    List<Schema.Field> fields = avroSchema.getFields();
    checkArgument(position >= 0 && position < fields.size(), "Invalid field position: " + position);
    // Object val = fields.get(position).getObjectProp(AvroSchemaUtil.FIELD_ID_PROP);
    // return val == null ? -1 : (int) val;
    throw new UnsupportedOperationException("NYI");
  }

  private AvroUtil() {}
}
