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

import java.util.HashMap;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

import io.delta.kernel.internal.data.GenericRow;

public class SingleAction {
    public static StructType SCHEMA = new StructType()
            .add("txn", new StructType())
            .add("add", AddFile.SCHEMA_WITH_STATS)
            .add("remove", new StructType())
            .add("metaData", Metadata.READ_SCHEMA)
            .add("protocol", Protocol.READ_SCHEMA)
            .add("cdc", new StructType())
            .add("commitInfo", CommitInfo.READ_SCHEMA);

    private static final int TXN_ORDINAL = SCHEMA.indexOf("txn");
    private static final int ADD_FILE_ORDINAL = SCHEMA.indexOf("add");
    private static final int REMOVE_FILE_ORDINAL = SCHEMA.indexOf("remove");
    private static final int METADATA_ORDINAL = SCHEMA.indexOf("metaData");
    private static final int PROTOCOL_ORDINAL = SCHEMA.indexOf("protocol");
    private static final int CDC_ORDINAL = SCHEMA.indexOf("cdc");
    private static final int COMMIT_INFO_ORDINAL = SCHEMA.indexOf("commitInfo");

    public static Row createAddFileSingleAction(Row addFile) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(ADD_FILE_ORDINAL, addFile);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    public static Row createProtocolSingleAction(Row protocol) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(PROTOCOL_ORDINAL, protocol);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    public static Row createMetadataSingleAction(Row metadata) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(METADATA_ORDINAL, metadata);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    public static Row createRemoveFileSingleAction(Row remove) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(REMOVE_FILE_ORDINAL, remove);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    public static Row createCommitInfoSingleAction(Row commitInfo) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(COMMIT_INFO_ORDINAL, commitInfo);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    public static Row createTxnSingleAction(Row txn) {
        Map<Integer, Object> singleActionValueMap = emptySingleActionValueMap();
        singleActionValueMap.put(TXN_ORDINAL, txn);
        return new GenericRow(SCHEMA, singleActionValueMap);
    }

    private static Map<Integer, Object> emptySingleActionValueMap() {
        Map<Integer, Object> singleActionValueMap = new HashMap<>();

        for (int i = 0; i < SCHEMA.length(); i++) {
            singleActionValueMap.put(0, null);
        }
        return singleActionValueMap;
    }
}
