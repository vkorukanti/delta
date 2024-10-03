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
package io.delta.kafka.channel;

import io.delta.kafka.DeltaSinkConfig;
import io.delta.kafka.events.DataComplete;
import io.delta.kafka.events.DataWritten;
import io.delta.kafka.events.TopicPartitionOffset;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = Lists.newArrayList();
  private final List<DataComplete> readyBuffer = Lists.newArrayList();
  private long startTime;
  private UUID currentCommitId;
  private final DeltaSinkConfig config;

  CommitState(DeltaSinkConfig config) {
    this.config = config;
  }

  void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      DataWritten dataWritten = (DataWritten) envelope.event().payload();
      LOG.warn(
          "Received commit response when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataWritten.commitId());
    }
  }

  void addReady(Envelope envelope) {
    DataComplete dataComplete = (DataComplete) envelope.event().payload();
    readyBuffer.add(dataComplete);
    if (!isCommitInProgress()) {
      LOG.warn(
          "Received commit ready when no commit in progress, this can happen during recovery. Commit ID: {}",
          dataComplete.commitId());
    }
  }

  UUID currentCommitId() {
    return currentCommitId;
  }

  boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  void clearResponses() {
    commitBuffer.clear();
  }

  boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    if (System.currentTimeMillis() - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Commit ID: {}", currentCommitId);
      return true;
    }
    return false;
  }

  boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  Map<String, List<Envelope>> tableCommitMap() {
    return Collections.singletonMap("temp", new ArrayList<>(commitBuffer));
  }

  OffsetDateTime validThroughTs(boolean partialCommit) {
    boolean hasValidThroughTs =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

    OffsetDateTime result;
    if (hasValidThroughTs) {
      result =
          readyBuffer.stream()
              .flatMap(event -> event.assignments().stream())
              .map(TopicPartitionOffset::timestamp)
              .min(Comparator.naturalOrder())
              .orElse(null);
    } else {
      result = null;
    }
    return result;
  }
}
