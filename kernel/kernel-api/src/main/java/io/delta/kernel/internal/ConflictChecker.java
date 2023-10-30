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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import io.delta.kernel.TransactionNonRetryableConflictException;
import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionWrapper;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.skipping.DataSkippingPredicate;
import io.delta.kernel.internal.skipping.DataSkippingUtils;
import io.delta.kernel.internal.util.*;

/**
 * Class containing the conflict resolution logic when writing to a Delta table.
 */
public class ConflictChecker {
    private ConflictChecker() {}

    public static TransactionRebaseState resolveConflicts(
            TableClient tableClient,
            SnapshotImpl snapshot,
            TransactionImpl transaction) throws TransactionNonRetryableConflictException {

        List<FileStatus> winningCommits = getWinningCommitFiles(
                tableClient.getFileSystemClient(),
                snapshot.getLogPath(),
                snapshot.getVersion(tableClient));

        ActionsIterator actionsIterator = new ActionsIterator(
                tableClient,
                winningCommits,
                SingleAction.SCHEMA, // TODO: scope for perf improvement by picking the fields,
                Optional.empty());

        Optional<Predicate> predicate =
                rewriteReadPredicate(transaction.getReadPredicate(), snapshot);

        Optional<PredicateEvaluator> predicateEvaluator = predicate.map(
                p -> tableClient.getExpressionHandler()
                        .getPredicateEvaluator(SingleAction.SCHEMA, p));

        while (actionsIterator.hasNext()) {
            ActionWrapper action = actionsIterator.next();
            Preconditions.checkArgument(!action.isFromCheckpoint());
            ColumnarBatch batch = action.getColumnarBatch();
            ColumnVector predicateResult = predicateEvaluator.map(
                    pe -> pe.eval(batch, Optional.empty()))
                    .orElse(null);

            CloseableIterator<Row> actionRows = batch.getRows();
            while (actionRows.hasNext()) {
                Row row = actionRows.next();

                // TODO: make this code cleaner
                if (!row.isNullAt(0)) {
                    // Handle SetTransaction
                    handleSetTransaction(row.getStruct(0), transaction);
                } else if (!row.isNullAt(1)) {
                    // Handle AddFile

                } else if (!row.isNullAt(2)) {
                    // Handle RemoveFile
                } else if (!row.isNullAt(3)) {
                    // Handle Metadata
                } else if (!row.isNullAt(4)) {
                    // Handle Protocol
                } else if (!row.isNullAt(5)) {
                    // Handle CDC
                } else if (!row.isNullAt(6)) {
                    // Handle CommitInfo
                }
            }
        }

        return new TransactionRebaseState(Collections.emptyList(), -1);
    }

    private static Optional<Predicate> rewriteReadPredicate(
            Optional<Predicate> readPredicateOpt, SnapshotImpl snapshot) {
        if (!readPredicateOpt.isPresent()) {
            return Optional.empty();
        }

        Predicate readPredicate = readPredicateOpt.get();
        // Split the filter into data and metadata filters
        Tuple2<Predicate, Predicate> metadataAndDataFilters =
                PartitionUtils.splitMetadataAndDataPredicates(
                        readPredicate, snapshot.getMetadata().getPartitionColNames());

        // Rewrite the data filter
        Optional<DataSkippingPredicate> dataSkippingPredicate =
                DataSkippingUtils.constructDataSkippingFilter(
                        metadataAndDataFilters._2,
                        snapshot.getMetadata().getDataSchema());
        Predicate skippingPredicate = dataSkippingPredicate.map(p -> (Predicate) p)
                .orElse(AlwaysTrue.ALWAYS_TRUE);

        // Combine the rewritten data filter with the metadata filter
        Predicate rewrittenPredicate = PartitionUtils.combineWithAndOp(
                metadataAndDataFilters._1,
                skippingPredicate);

        return Optional.of(rewrittenPredicate);
    }

    public Predicate rewriteAsSubField() {
        return null;
    }

    private static void handleSetTransaction(Row setTxn, TransactionImpl transaction) {

    }

    private static void handleAddFile(Row addFile, boolean predicateResult) {

    }

    private static void handleRemoveFile(
            Row removeFile,
            boolean predicateResult,
            List<String> removedFiles) {

    }

    private static void handleProtocol(Row protocol, TransactionImpl transaction) {

    }

    private static void handleMetadata(Row metadata, TransactionImpl transaction) {

    }

    /**
     * Class containing the rebase state from winning transactions that the current
     * transaction needs to rebase against before attempting the commit.
     */
    public static class TransactionRebaseState {
        private final List<String> deletedFiles;
        private final long latestVersion;
        public TransactionRebaseState(List<String> deletedFiles, long latestVersion) {
            this.deletedFiles = deletedFiles; // TODO: immutable copy
            this.latestVersion = latestVersion;
        }

        /**
         * Return the list of files deleted by the winning transactions.
         *
         * @return List of file paths deleted by the winning transactions.
         */
        public List<String> getDeletedFiles() {
            return deletedFiles;
        }

        /**
         * Return the latest winning version of the table.
         *
         * @return latest winning version of the table.
         */
        public long getLatestVersion() {
            return latestVersion;
        }
    }

    private static List<FileStatus> getWinningCommitFiles(
            FileSystemClient fsClient,
            Path logPath,
            long readVersion) {
        String firstWinningCommitFile = FileNames.deltaFile(logPath, readVersion + 1);

        try {
            CloseableIterator<FileStatus> files = fsClient.listFrom(firstWinningCommitFile);

            // Filter out all winning transaction commit files.
            List<FileStatus> winningCommitFiles = new ArrayList<>();
            while (files.hasNext()) {
                FileStatus file = files.next();
                if (FileNames.isCommitFile(file.getPath())) {
                    winningCommitFiles.add(files.next());
                }
            }
            // TODO: error handling to see if there are any gaps, i.e a missing winning commit file
            return winningCommitFiles;
        } catch (FileNotFoundException nfe) {
            // no winning commits. why did we get here?
            throw new IllegalStateException("No winning commits found.", nfe);
        } catch (IOException ioe) {
            throw new RuntimeException("Error listing files from " + firstWinningCommitFile, ioe);
        }
    }
}
