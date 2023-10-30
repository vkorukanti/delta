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
package io.delta.kernel;

import java.util.Map;
import java.util.Set;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;

public interface TransactionBuilder {
    /**
     * Set the new schema of the table. This is used when creating the table for the first
     * time or when changing the schema of the table.
     *
     * @param tableClient
     * @param newTableSchema
     * @return
     */
    TransactionBuilder withSchema(TableClient tableClient, StructType newTableSchema);

    /**
     * Set the partition columns of the table. Partition columns can only be set when creating
     * the table for the first time. Subsequent updates to the partition columns are not allowed.
     * @param tableClient
     * @param partitionColumns
     * @return
     */
    TransactionBuilder withPartitionColumns(
            TableClient tableClient,
            Set<String> partitionColumns);

    /**
     * Set the transaction identifier for idempotent writes.
     *
     * @param tableClient
     * @param applicationId
     * @param transactionVersion
     * @return
     */
    TransactionBuilder withTransactionId(
        TableClient tableClient,
        String applicationId,
        long transactionVersion);

    /**
     * Set the predicate of what qualifying files from and what version of the table are read in
     * order to generate the updates for this transaction.
     *
     * @param tableClient
     * @param readVersion What version of the table is read for generating the updates?
     * @param predicate What files are read for generating the updates
     * @return
     */
    TransactionBuilder withReadSet(
            TableClient tableClient,
            long readVersion,
            Predicate predicate);

    /**
     * Set the given new table properties on the table as part of the transaction.
     *
     * @param tableClient
     * @param tableProperties
     * @return
     */
    TransactionBuilder withNewTableProperties(
        TableClient tableClient,
        Map<String, String> tableProperties);

    /**
     * Unset the given table properties on the table as part of the transaction.
     *
     * @param tableClient
     * @param tablePropertyKeys
     * @return
     */
    TransactionBuilder withRemovedTableProperties(
        TableClient tableClient,
        Set<String> tablePropertyKeys);

    /**
     * Build the transaction.
     *
     * Could throw errors
     * - TransactionAlreadyExecutedException
     * - UnsupportedTableProperty
     * - NoSuchTablePropertyExists
     * - InvalidSchemaChangeException
     */
    Transaction build(TableClient tableClient);
}
