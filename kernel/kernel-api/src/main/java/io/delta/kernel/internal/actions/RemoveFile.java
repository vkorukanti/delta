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
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.util.InternalUtils.relativizePath;
import static io.delta.kernel.internal.util.PartitionUtils.serializePartitionMap;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/** Metadata about {@code remove} action in the Delta Log. */
public class RemoveFile {
  /** Full schema of the {@code remove} action in the Delta Log. */
  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("path", StringType.STRING, false /* nullable */)
          .add("deletionTimestamp", LongType.LONG, true /* nullable */)
          .add("dataChange", BooleanType.BOOLEAN, false /* nullable*/)
          .add("extendedFileMetadata", BooleanType.BOOLEAN, true /* nullable */)
          .add(
              "partitionValues",
              new MapType(StringType.STRING, StringType.STRING, true),
              true /* nullable*/)
          .add("size", LongType.LONG, true /* nullable*/)
          .add("stats", StringType.STRING, true /* nullable */)
          .add("tags", new MapType(StringType.STRING, StringType.STRING, true), true /* nullable */)
          .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);
  // There are more fields which are added when row-id tracking is enabled. When Kernel
  // starts supporting row-ids, we should add those fields here.

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

  /**
   * Utility to generate `RemoveFile` row from the given {@link DataFileStatus} and partition
   * values.
   */
  public static Row convertDataFileStatus(
      URI tableRoot,
      DataFileStatus dataFileStatus,
      Map<String, Literal> partitionValues,
      boolean dataChange) {
    Path filePath = new Path(dataFileStatus.getPath());
    Map<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("path"), relativizePath(filePath, tableRoot).toUri().toString());
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("partitionValues"), serializePartitionMap(partitionValues));
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("deletionTimestamp"), dataFileStatus.getModificationTime());
    valueMap.put(COL_NAME_TO_ORDINAL.get("dataChange"), dataChange);

    // any fields not present in the valueMap are considered null
    return new GenericRow(FULL_SCHEMA, valueMap);
  }
}
