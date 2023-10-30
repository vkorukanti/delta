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

import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Contains the result of a transaction commit. Returned by
 * {@link Transaction#commit(TableClient, CloseableIterator, Optional)}.
 */
public class TransactionCommitStatus {
    private final boolean isCommitted;
    private final long version;
    private final boolean canCheckpoint;

    public TransactionCommitStatus(
            boolean isCommitted,
            long version,
            boolean canCheckpoint) {
        this.isCommitted = isCommitted;
        this.version = version;
        this.canCheckpoint = canCheckpoint;
    }

    /**
     * Returns whether the transaction was committed successfully.
     *
     * @return true if the transaction is successful, false when the transaction needs
     * to be retried.
     */
    public boolean isCommitted() {
        return isCommitted;
    }

    /**
     * If the transaction is successful, this contains the version of the transaction committed as.
     * Otherwise, it contains the latest transaction of the table, the connector needs to use to
     * re-attempt the transaction commit as version + 1.
     *
     * @return latest version of the table.
     */
    public long getVersion() {
        return version;
    }

    /**
     * Is the table ready for checkpoint (i.e. there are enough commits since the last checkpoint)?
     * If yes the connector can choose to checkpoint as the version the transaction is committed as.
     *
     * @return Is the table ready for checkpointing?
     */
    public boolean isCanCheckpoint() {
        return canCheckpoint;
    }
}

