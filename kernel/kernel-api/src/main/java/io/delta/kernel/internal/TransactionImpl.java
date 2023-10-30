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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.*;
import static io.delta.kernel.internal.util.Preconditions.checkState;

public class TransactionImpl implements Transaction {
    public static final int DEFAULT_READ_VERSION = 1;
    public static final int DEFAULT_WRITE_VERSION = 2;

    private final String engineInfo;
    private final String operation;
    private final Path dataPath;
    private final Path logPath;
    private final Optional<StructType> newSchema;
    private final Protocol protocol;
    private final Metadata metadata;
    private final Optional<Long> readVersion;
    private final Optional<Predicate> readPredicate;
    private final SnapshotImpl readSnapshot;

    // Set transaction identifier
    private final Optional<String> applicationId;
    private final Optional<Long> txnVersion;

    // To avoid trying to commit the same transaction again.
    private boolean committed;

    public TransactionImpl(
            Path dataPath,
            Path logPath,
            SnapshotImpl readSnapshot,
            String engineInfo,
            String operation,
            Optional<StructType> newSchema,
            Protocol protocol,
            Metadata metadata,
            Optional<Long> readVersion,
            Optional<Predicate> readPredicate,
            Optional<String> applicationId,
            Optional<Long> txnVersion) {
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.readSnapshot = readSnapshot;
        this.engineInfo = engineInfo;
        this.operation = operation;
        this.newSchema = newSchema;
        this.protocol = protocol;
        this.metadata = metadata;
        this.readVersion = readVersion;
        this.readPredicate = readPredicate;
        this.applicationId = applicationId;
        this.txnVersion = txnVersion;
    }

    @Override
    public Row getState(TableClient tableClient) {
        StructType tableSchema = readSnapshot.getSchema(tableClient);
        // Physical equivalent of the logical read schema.
        StructType physicalReadSchema = ColumnMapping.convertToPhysicalSchema(
                tableSchema,
                tableSchema,
                ColumnMapping.getColumnMappingMode(metadata.getConfiguration()));

        return TransactionStateRow.of(
                metadata,
                protocol,
                // TODO: we just need to pass one schema `tableSchema` which has all the
                // physical schema related properties
                tableSchema.toJson(),
                physicalReadSchema.toJson(),
                dataPath.toString());
    }

    @Override
    public List<String> getPartitionColumns(TableClient tableClient) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StructType getSchema(TableClient tableClient) {
        return newSchema.orElseGet(() -> readSnapshot.getSchema(tableClient));
    }

    @Override
    public TransactionCommitStatus commit(
            TableClient tableClient,
            CloseableIterator<Row> stagedData,
            Optional<TransactionCommitStatus> previousCommitStatus)
            throws TransactionNonRetryableConflictException{
        checkState(!committed, "Transaction is already committed. Create a new transaction.");
        List<Row> metadataActions = new ArrayList<>();
        metadataActions.add(SingleAction.createCommitInfoSingleAction(generateCommitAction()));
        metadataActions.add(SingleAction.createMetadataSingleAction(Metadata.toRow(metadata)));
        metadataActions.add(SingleAction.createProtocolSingleAction(Protocol.toRow(protocol)));
        addSetTransactionIfPresent(metadataActions);

        // Create a new CloseableIterator that will return the commit action, metadata action, and
        // the stagedData
        CloseableIterator<Row> stagedDataWithCommitInfo = new CloseableIterator<Row>() {
            private int metadataActionReturned = -1;

            @Override
            public boolean hasNext() {
                return metadataActionReturned < metadataActions.size() - 1 ||
                        stagedData.hasNext();
            }

            @Override
            public Row next() {
                if (metadataActionReturned < metadataActions.size() - 1) {
                    metadataActionReturned += 1;
                    return metadataActions.get(metadataActionReturned);
                } else {
                    return stagedData.next();
                }
            }

            @Override
            public void close() throws IOException {
                stagedData.close();
            }
        };

        try {
            long readVersion = readSnapshot.getVersion(tableClient);
            if (readVersion == -1) {
                // New table, create a delta log directory
                tableClient.getFileSystemClient().mkdir(logPath.toString());
            }

            long newVersion = readVersion + 1;
            /**
             * metadata tag, actions
             * Commit
             */
            // Write the staged data to a delta file
            tableClient.getJsonHandler().writeJsonFileAtomically(
                    FileNames.deltaFile(logPath, newVersion),
                    stagedDataWithCommitInfo
            );

            committed = true;

            return new TransactionCommitStatus(
                    true /* isCommitted */,
                    newVersion,
                    // TODO: hack until we add code to properly check the checkpoint interval and
                    // last checkpoint
                    newVersion > 0 && newVersion % 10 == 0);
        } catch (FileAlreadyExistsException e) {
            ConflictChecker.TransactionRebaseState rebaseStatus =
                    ConflictChecker.resolveConflicts(tableClient, readSnapshot, this);
            throw new TransactionNonRetryableConflictException();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public Optional<Long> getReadVersion() {
        return readVersion;
    }

    public Optional<Predicate> getReadPredicate() {
        return readPredicate;
    }

    /**
     * Get the part of the schema of the table that needs the statistics to be collected per file.
     *
     * @param tableClient
     * @param transactionState
     * @return
     */
    public static List<Column> getStatisticsColumns(TableClient tableClient, Row transactionState) {

        long statsColumnIndex = TransactionStateRow.getDataSkippingColIndex(transactionState);
        if (statsColumnIndex <= 0) {
            throw new UnsupportedOperationException(
                    "TODO: we should collect stats for all columns in the schema");
        }
        List<Column> statsColumns = new ArrayList<>();
        StructType physicalSchema =
                TransactionStateRow.getPhysicalSchema(tableClient, transactionState);
        List<String> partitionColumns = TransactionStateRow.getPartitionColumns(transactionState);
        Set<String> partitionColPhysicalNames =
                TransactionStateRow.getLogicalSchema(tableClient, transactionState)
                        .fields().stream()
                        .filter(field -> partitionColumns.contains(
                                field.getName().toLowerCase(Locale.ROOT)))
                        .map(field -> ColumnMapping.getPhysicalName(field)
                                .toLowerCase(Locale.ROOT))
                        .collect(toSet());

        for (int index = 0; index < statsColumnIndex && index < physicalSchema.length(); index++) {
            StructField structField = physicalSchema.at(index);
            DataType dataType = structField.getDataType();
            if (dataType instanceof StructType ||
                    dataType instanceof ArrayType ||
                    dataType instanceof MapType) {
                throw new UnsupportedOperationException(
                        "TODO: support complex type stats collection");
            }

            if (partitionColPhysicalNames.contains(
                    structField.getName().toLowerCase(Locale.ROOT))) {
                continue;
            }

            statsColumns.add(new Column(structField.getName().toLowerCase(Locale.ROOT)));
        }

        return statsColumns;
    }

    public static String getTargetDirectory(
            Row transactionState,
            Map<String, Literal> partitionValues) {
        Path targetDirectory = new Path(TransactionStateRow.getTableRoot(transactionState));
        for (Map.Entry<String, Literal> entry : partitionValues.entrySet()) {
            String partitionDirectory =
                    entry.getKey() + "=" + PartitionUtils.serializePartitionValue(entry.getValue());
            targetDirectory = new Path(targetDirectory, partitionDirectory);
        }

        return targetDirectory.toString();
    }

    private Row generateCommitAction() {
        Map<Integer, Object> commitInfo = new HashMap<>();
        commitInfo.put(0, System.currentTimeMillis());
        commitInfo.put(1, engineInfo);
        commitInfo.put(2, operation);

        return new GenericRow(CommitInfo.READ_SCHEMA, commitInfo);
    }

    private void addSetTransactionIfPresent(List<Row> actions) {
        if (applicationId.isPresent() && txnVersion.isPresent()) {
            actions.add(
                    SingleAction.createTxnSingleAction(
                            SetTransaction.toRow(
                                    new SetTransaction(
                                            applicationId.get(),
                                            txnVersion.get(),
                                            Optional.of(System.currentTimeMillis())))));
        }
    }
}
