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
package io.delta.kernel.internal.data;

import java.util.*;
import java.util.stream.IntStream;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Transaction;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.VectorUtils;

public class TransactionStateRow extends GenericRow {

    private static final StructType SCHEMA = new StructType()
            .add("configuration", new MapType(StringType.STRING, StringType.STRING, false))
            .add("logicalSchemaString", StringType.STRING)
            .add("physicalSchemaString", StringType.STRING)
            .add("partitionColumns", new ArrayType(StringType.STRING, false))
            .add("minReaderVersion", IntegerType.INTEGER)
            .add("minWriterVersion", IntegerType.INTEGER)
            .add("tablePath", StringType.STRING);

    private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
            IntStream.range(0, SCHEMA.length())
                    .boxed()
                    .collect(toMap(i -> SCHEMA.at(i).getName(), i -> i));

    public static TransactionStateRow of(
            Metadata metadata,
            Protocol protocol,
            String readSchemaLogicalJson,
            String readSchemaPhysicalJson,
            String tablePath) {
        HashMap<Integer, Object> valueMap = new HashMap<>();
        valueMap.put(COL_NAME_TO_ORDINAL.get("configuration"), metadata.getConfigurationMapValue());
        valueMap.put(COL_NAME_TO_ORDINAL.get("logicalSchemaString"), readSchemaLogicalJson);
        valueMap.put(COL_NAME_TO_ORDINAL.get("physicalSchemaString"), readSchemaPhysicalJson);
        valueMap.put(COL_NAME_TO_ORDINAL.get("partitionColumns"), metadata.getPartitionColumns());
        valueMap.put(COL_NAME_TO_ORDINAL.get("minReaderVersion"), protocol.getMinReaderVersion());
        valueMap.put(COL_NAME_TO_ORDINAL.get("minWriterVersion"), protocol.getMinWriterVersion());
        valueMap.put(COL_NAME_TO_ORDINAL.get("tablePath"), tablePath);
        return new TransactionStateRow(valueMap);
    }

    private TransactionStateRow(HashMap<Integer, Object> valueMap) {
        super(SCHEMA, valueMap);
    }

    /**
     * Utility method to get the logical schema from the write state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     *
     * @param tableClient      instance of {@link TableClient} to use.
     * @param transactionState Scan state {@link Row}
     * @return Logical schema to read from the data files.
     */
    public static StructType getLogicalSchema(TableClient tableClient, Row transactionState) {
        String serializedSchema =
                transactionState.getString(COL_NAME_TO_ORDINAL.get("logicalSchemaString"));
        return tableClient.getJsonHandler().deserializeStructType(serializedSchema);
    }

    /**
     * Utility method to get the physical schema from the write state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     *
     * @param tableClient      instance of {@link TableClient} to use.
     * @param transactionState Scan state {@link Row}
     * @return Physical schema to read from the data files.
     */
    public static StructType getPhysicalSchema(TableClient tableClient, Row transactionState) {
        String serializedSchema =
                transactionState.getString(COL_NAME_TO_ORDINAL.get("physicalSchemaString"));
        return tableClient.getJsonHandler().deserializeStructType(serializedSchema);
    }

    /**
     * Get the list of partition column names from the write state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     *
     * @param transactionState Scan state {@link Row}
     * @return List of partition column names according to the scan state.
     */
    public static List<String> getPartitionColumns(Row transactionState) {
        return VectorUtils.toJavaList(
                transactionState.getArray(COL_NAME_TO_ORDINAL.get("partitionColumns")));
    }

    /**
     * Get the column mapping mode from the scan state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     */
    public static String getColumnMappingMode(Row transactionState) {
        Map<String, String> configuration = VectorUtils.toJavaMap(
                transactionState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));
        return ColumnMapping.getColumnMappingMode(configuration);
    }

    /**
     * Get the target file size from the table transaction state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     */
    public static long getTargetFileSize(Row transactionState) {
        Map<String, String> configuration = VectorUtils.toJavaMap(
                transactionState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));
        String targetFileSize = configuration
                .getOrDefault("delta.targetFileSize", Long.valueOf(128 * 1024 * 1024).toString());

        return Long.parseLong(targetFileSize);
    }

    /**
     * Get the list of columns to collect stats from the transaction state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     */
    public static long getDataSkippingColIndex(Row transactionState) {
        Map<String, String> configuration = VectorUtils.toJavaMap(
                transactionState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));

        // comma separated list of column names
        String stasColIndex = configuration.get("delta.dataSkippingNumIndexedCols");

        if (stasColIndex == null || stasColIndex.isEmpty()) {
            return 32; // default value
        }

        return Long.parseLong(stasColIndex);
    }

    /**
     * Get the list of columns to collect stats from the transaction state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}.
     */
    public static Optional<List<String>> getDataSkippingStatsColumns(Row transactionState) {
        Map<String, String> configuration = VectorUtils.toJavaMap(
                transactionState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));

        // comma separated list of column names
        String statsColumns = configuration.get("delta.dataSkippingStatsColumns");

        if (statsColumns == null || statsColumns.isEmpty()) {
            return Optional.empty();
        }

        // TODO: do we need to do any validation that the columns actually exists in the schema?
        return Optional.of(Arrays.asList(statsColumns.split(",")));
    }

    /**
     * Get the table root from scan state {@link Row} returned by
     * {@link Transaction#getState(TableClient)}
     *
     * @param transactionState Transaction state state {@link Row}
     * @return Fully qualified path to the location of the table.
     */
    public static String getTableRoot(Row transactionState) {
        return transactionState.getString(COL_NAME_TO_ORDINAL.get("tablePath"));
    }
}
