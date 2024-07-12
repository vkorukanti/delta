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
import java.util.*;

import io.delta.kernel.*;
import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import static io.delta.kernel.internal.actions.SingleAction.CHANGES_SCHEMA;
import static io.delta.kernel.internal.fs.Path.getName;

public class TableImpl implements Table {
    public static Table forPath(Engine engine, String path) {
        return forPath(engine, path, System::currentTimeMillis);
    }

    /**
     * Instantiate a table object for the Delta Lake table at the given path. It takes an additional
     * parameter called {@link Clock} which helps in testing.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @param path   location of the table.
     * @param clock  {@link Clock} instance to use for time-related operations.
     * @return an instance of {@link Table} representing the Delta table at the given path
     */
    public static Table forPath(Engine engine, String path, Clock clock) {
        String resolvedPath;
        try {
            resolvedPath = engine.getFileSystemClient().resolvePath(path);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        return new TableImpl(resolvedPath, clock);
    }

    private final SnapshotManager snapshotManager;
    private final String tablePath;
    private final Clock clock;

    public TableImpl(String tablePath, Clock clock) {
        this.tablePath = tablePath;
        final Path dataPath = new Path(tablePath);
        final Path logPath = new Path(dataPath, "_delta_log");
        this.snapshotManager = new SnapshotManager(logPath, dataPath);
        this.clock = clock;
    }

    @Override
    public String getPath(Engine engine) {
        return tablePath;
    }

    @Override
    public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
        return snapshotManager.buildLatestSnapshot(engine);
    }

    @Override
    public Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
            throws TableNotFoundException {
        return snapshotManager.getSnapshotAt(engine, versionId);
    }

    @Override
    public Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
            throws TableNotFoundException {
        return snapshotManager.getSnapshotForTimestamp(engine, millisSinceEpochUTC);
    }

    @Override
    public void checkpoint(Engine engine, long version)
            throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
        snapshotManager.checkpoint(engine, version);
    }

    @Override
    public TransactionBuilder createTransactionBuilder(
            Engine engine,
            String engineInfo,
            Operation operation) {
        return new TransactionBuilderImpl(this, engineInfo, operation);
    }

    public Clock getClock() {
        return clock;
    }

    protected Path getDataPath() {
        return new Path(tablePath);
    }

    protected Path getLogPath() {
        return new Path(tablePath, "_delta_log");
    }

    @Override
    public CloseableIterator<ActionWrapper> getChanges(
            Engine engine,
            Row streamingState,
            StreamingRemoveHandlingPolicy removeHandlingPolicy,
            long startVersion,
            long endVersion) {
        Metadata stateMetadata = Metadata.fromRow(engine, streamingState);
        try (CloseableIterator<FileStatus> logFiles = engine
                .getFileSystemClient()
                .listFrom(FileNames.listingPrefix(getLogPath(), startVersion))) {

            List<FileStatus> commitFileToRead = new ArrayList<>();
            while (logFiles.hasNext()) {
                FileStatus fileStatus = logFiles.next();
                if (FileNames.isCommitFile(getName(fileStatus.getPath()))) {
                    long currentVersion = FileNames.getFileVersion(new Path(fileStatus.getPath()));
                    if (currentVersion >= startVersion && currentVersion <= endVersion) {
                        commitFileToRead.add(fileStatus);
                    }
                }
            }

            ActionsIterator actionsIterator = new ActionsIterator(
                    engine,
                    commitFileToRead,
                    CHANGES_SCHEMA,
                    Optional.empty() /* checkpointPredicate */);

            return actionsIterator.map(actionWrapper -> {
                // Check the schema has not changed.
                ColumnarBatch batch = actionWrapper.getColumnarBatch();
                ColumnVector metadataColumn = batch.getColumnVector(2 /* metadata col ordinal */);
                for (int rowId = 0; rowId < metadataColumn.getSize(); rowId++) {
                    if (!metadataColumn.isNullAt(rowId)) {
                        Metadata newMetadata =
                                Metadata.fromColumnVector(metadataColumn, rowId, engine);
                        if (!newMetadata.getSchema().equivalent(stateMetadata.getSchema())) {
                            throw new KernelException(
                                    "Metadata has changed between versions. " +
                                            "Expected: " + stateMetadata + " Found: " +
                                            newMetadata);
                        }
                    }
                }

                ColumnVector removeColumn = batch.getColumnVector(1 /* remove col ordinal */);
                for (int rowId = 0; rowId < removeColumn.getSize(); rowId++) {
                    if (!removeColumn.isNullAt(rowId)) {
                        if (removeHandlingPolicy ==
                                StreamingRemoveHandlingPolicy.ERROR_ON_REMOVED_FILES) {
                            throw new KernelException("Encountered a remove file");
                        }
                    }
                }

                return actionWrapper; // TODO: should remove the metadata and protocol columns
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
