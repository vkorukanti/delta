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

import io.delta.kernel.*;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
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
            Optional<Predicate> readPredicate) {
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
        // Prepare the commit info action
        Row commitAction = SingleAction.createCommitInfoSingleAction(generateCommitAction());
        // Prepare the metadata action
        Row metadataAction = SingleAction.createMetadataSingleAction(Metadata.toRow(metadata));
        // Prepare the protocol action
        Row protocolAction = SingleAction.createProtocolSingleAction(Protocol.toRow(protocol));

        // Create a new CloseableIterator that will return the commit action, metadata action, and
        // the stagedData
        CloseableIterator<Row> stagedDataWithCommitInfo = new CloseableIterator<Row>() {
            private boolean commitInfoReturned = false;
            private boolean metadataReturned = false;
            private boolean protocolReturned = false;

            @Override
            public boolean hasNext() {
                return !commitInfoReturned ||
                        !metadataReturned ||
                        !protocolReturned ||
                        stagedData.hasNext();
            }

            @Override
            public Row next() {
                if (!commitInfoReturned) {
                    commitInfoReturned = true;
                    return commitAction;
                } else if (!metadataReturned) {
                    metadataReturned = true;
                    return metadataAction;
                } else if (!protocolReturned) {
                    protocolReturned = true;
                    return protocolAction;
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
            throw new TransactionNonRetryableConflictException();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private Row generateCommitAction() {
        Map<Integer, Object> commitInfo = new HashMap<>();
        commitInfo.put(0, System.currentTimeMillis());
        commitInfo.put(1, engineInfo);
        commitInfo.put(2, operation);

        return new GenericRow(CommitInfo.READ_SCHEMA, commitInfo);
    }
}
