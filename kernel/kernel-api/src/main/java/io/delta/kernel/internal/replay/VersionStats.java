/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import static java.util.stream.Collectors.toMap;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class VersionStats {

  public static VersionStats fromColumnarBatch(long version, ColumnarBatch batch, int rowId) {
    // fromColumnVector already takes care of nulls
    Protocol protocol =
        Protocol.fromColumnVector(
            batch.getColumnVector(COL_NAME_TO_ORDINAL.get("protocol")), rowId);
    Metadata metadata =
        Metadata.fromColumnVector(
            batch.getColumnVector(COL_NAME_TO_ORDINAL.get("metadata")), rowId);
    return new VersionStats(version, metadata, protocol);
  }

  // We can add additional fields later
  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("version", LongType.LONG)
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("metadata", Metadata.FULL_SCHEMA);

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, FULL_SCHEMA.length())
          .boxed()
          .collect(toMap(i -> FULL_SCHEMA.at(i).getName(), i -> i));

  private final long version;
  private final Metadata metadata;
  private final Protocol protocol;

  public VersionStats(long version, Metadata metadata, Protocol protocol) {
    this.version = version;
    this.metadata = metadata;
    this.protocol = protocol;
  }

  /** The version of the Delta table that this VersionStats represents. */
  public long getVersion() {
    return version;
  }

  /** The {@link Metadata} stored in this VersionStats. May be null. */
  public Metadata getMetadata() {
    return metadata;
  }

  /** The {@link Protocol} stored in this VersionStats. May be null. */
  public Protocol getProtocol() {
    return protocol;
  }

  public Row toRow() {
    Map<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(COL_NAME_TO_ORDINAL.get("version"), version);
    valueMap.put(COL_NAME_TO_ORDINAL.get("protocol"), protocol.toRow());
    valueMap.put(COL_NAME_TO_ORDINAL.get("metadata"), metadata.toRow());

    // any fields not present in the valueMap are considered null
    return new GenericRow(FULL_SCHEMA, valueMap);
  }
}
