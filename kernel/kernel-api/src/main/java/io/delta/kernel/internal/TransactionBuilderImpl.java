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
package io.delta.kernel.internal;

import java.util.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.util.Tuple2;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;

public class TransactionBuilderImpl implements TransactionBuilder {
    private final TableImpl table;
    private final String engineInfo;
    private final String operation;
    private Optional<StructType> newSchema = Optional.empty();
    private Optional<Set<String>> partitionColumns = Optional.empty();
    private Optional<Predicate> readPredicate = Optional.empty();
    private Optional<Long> readVersion = Optional.empty();

    // SetTransaction identifiers for idempotent writes
    private Optional<String> applicationId = Optional.empty();
    private Optional<Long> txnVersion = Optional.empty();

    public TransactionBuilderImpl(TableImpl table, String engineInfo, String operation) {
        this.table = table;
        this.engineInfo = engineInfo;
        this.operation = operation;
    }

    @Override
    public TransactionBuilder withSchema(TableClient tableClient, StructType newTableSchema) {
        this.newSchema = Optional.of(newTableSchema);
        // TODO: this needs to verified
        return this;
    }

    @Override
    public TransactionBuilder withPartitionColumns(
            TableClient tableClient,
            Set<String> partitionColumns) {
        if (!partitionColumns.isEmpty()) {
            this.partitionColumns = Optional.of(partitionColumns);
        }
        return this;
    }

    @Override
    public TransactionBuilder withReadSet(
            TableClient tableClient,
            long readVersion,
            Predicate readPredicate) {
        this.readVersion = Optional.of(readVersion);
        this.readPredicate = Optional.of(readPredicate);
        return this;
    }

    @Override
    public TransactionBuilder withTransactionId(
            TableClient tableClient,
            String applicationId,
            long transactionVersion) {
        this.applicationId =
                Optional.of(requireNonNull(applicationId, "applicationId is null"));
        this.txnVersion = Optional.of(transactionVersion);
        return this;
    }

    @Override
    public TransactionBuilder withNewTableProperties(
        TableClient tableClient,
        Map<String, String> tableProperties) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public TransactionBuilder withRemovedTableProperties(
        TableClient tableClient,
        Set<String> tablePropertyKeys) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Transaction build(TableClient tableClient) {
        SnapshotImpl snapshot;
        try {
            snapshot = (SnapshotImpl) table.getLatestSnapshot(tableClient);
        } catch (TableNotFoundException tblf) {
            newSchema.orElseThrow(() ->
                    new IllegalArgumentException(
                            "Table doesn't exist yet. Must provide a new schema."));
            // Table doesn't exist yet. Create an initial snapshot with the new schema.
            Metadata metadata = getInitialMetadata();
            Protocol protocol = getInitialProtocol();
            LogReplay logReplay = getEmptyLogReplay(tableClient, metadata, protocol);
            snapshot = new InitialSnapshot(
                    table.getLogPath(),
                    table.getDataPath(),
                    logReplay,
                    metadata,
                    protocol);
        }
        validate(tableClient, snapshot);
        return new TransactionImpl(
                table.getDataPath(),
                table.getLogPath(),
                snapshot,
                engineInfo,
                operation,
                newSchema,
                // TODO: need to construct new protocol and metadata objects if the
                // transaction has any properties that are changed.
                snapshot.getProtocol(),
                snapshot.getMetadata(),
                readVersion,
                readPredicate,
                applicationId,
                txnVersion);
    }

    /**
     * Validate the given parameters for the transaction.
     */
    private void validate(TableClient tableClient, SnapshotImpl snapshot) {
        long currentSnapshotVersion = snapshot.getVersion(tableClient);
        if (readVersion.isPresent() && readVersion.get() != currentSnapshotVersion) {
            throw new IllegalArgumentException(
                    "Read version doesn't match the current snapshot version. " +
                            "Read version: " + readVersion.get() +
                            " Current snapshot version: " + currentSnapshotVersion);
        }
        if (currentSnapshotVersion >= 0 && partitionColumns.isPresent()) {
            throw new IllegalArgumentException(
                    "Partition columns can only be set on a table that doesn't exist yet.");
        }

        if (txnVersion.isPresent() && applicationId.isPresent()) {
            Optional<Long> lastTxnVersion =
                    snapshot.getLatestTransactionVersion(applicationId.get());
            if (lastTxnVersion.isPresent() && lastTxnVersion.get() >= txnVersion.get()) {
                // TODO: change this to use the new KernelExceptions once they are available
                throw new IllegalArgumentException(
                        "Transaction with identifier is already committed. " +
                                "ApplicationId: " + applicationId.get() +
                                ", Transaction version: " + txnVersion.get() +
                                ". Last committed transaction version: " + lastTxnVersion.get());
            }
        }

        // TODO: need to validate the protocol and metadata options
        // TODO: if a new schema is given make sure it is valid
        // TODO: if a table property is removed, make sure it is valid
        // TODO: if a table property is added, make sure it is valid
    }


    private class InitialSnapshot extends SnapshotImpl {
        InitialSnapshot(
                Path logPath,
                Path dataPath,
                LogReplay logReplay,
                Metadata metadata,
                Protocol protocol) {
            super(logPath, dataPath, -1 /* version */, logReplay, protocol, metadata);
        }
    }

    private LogReplay getEmptyLogReplay(
            TableClient tableClient,
            Metadata metadata,
            Protocol protocol) {
        return new LogReplay(
                table.getLogPath(),
                table.getDataPath(),
                -1,
                tableClient,
                LogSegment.empty(table.getLogPath()),
                Optional.empty()) {

            @Override
            protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
                    Optional<SnapshotHint> snapshotHint, long snapshotVersion) {
                return new Tuple2<>(protocol, metadata);
            }
        };
    }

    private Metadata getInitialMetadata() {
        return new Metadata(
                java.util.UUID.randomUUID().toString(), /* id */
                Optional.empty(), /* name */
                Optional.empty(), /* description */
                new Format(), /* format */
                newSchema.get().toJson(), /* schemaString */
                newSchema.get(), /* schema */
                stringArrayValue(partitionColumns.isPresent() ?
                        partitionColumns.get().stream().collect(toList()) :
                        Collections.emptyList()), /* partitionColumns */
                Optional.of(System.currentTimeMillis()), /* createdTime */
                stringStringMapValue(Collections.emptyMap()) /* configuration */
        );
    }

    private Protocol getInitialProtocol() {
        return new Protocol(
                TransactionImpl.DEFAULT_READ_VERSION,
                TransactionImpl.DEFAULT_WRITE_VERSION,
                null,
                null);
    }
}
