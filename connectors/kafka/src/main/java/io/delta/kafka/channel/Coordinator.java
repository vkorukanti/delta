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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kafka.DeltaSinkConfig;
import io.delta.kafka.events.CommitComplete;
import io.delta.kafka.events.CommitToTable;
import io.delta.kafka.events.DataWritten;
import io.delta.kafka.events.Event;
import io.delta.kafka.events.StartCommit;
import io.delta.kafka.util.Utils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final DeltaSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private volatile boolean terminated;

  Coordinator(
      DeltaSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.connectGroupId() + "-coord", config, clientFactory, context);

    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.exec = Utils.newWorkerPool("iceberg-committer", 2 /* TODO: make configurable */);
    this.commitState = new CommitState(config);
  }

  void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Commit {} initiated", commitState.currentCommitId());
    }

    consumeAvailable(POLL_DURATION);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<String, List<Envelope>> commitMap = commitState.tableCommitMap();

    String offsetsJson = offsetsJson();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);

    commitMap.forEach(
        (key, value) -> {
          commitToTable(key, value, offsetsJson, validThroughTs);
        });

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), valid-through {}",
        commitState.currentCommitId(),
        commitMap.size(),
        validThroughTs);
  }

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsets());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void commitToTable(
      String tableName,
      List<Envelope> envelopeList,
      String offsetsJson,
      OffsetDateTime validThroughTs) {

    List<DataWritten> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  // TODO: figureout the last committed offsets
                  Long minOffset = null; // committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .collect(Collectors.toList());

    List<String> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .collect(Collectors.toList());

    if (terminated) {
      throw new ConnectException("Coordinator is terminated, commit aborted");
    }

    if (dataFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableName);
    } else {

      // TODO: this is where we should commit the files

      Long snapshotId = 0L; // fetch the snapshot ID from the table
      Event event =
          new Event(
              config.connectGroupId(),
              new CommitToTable(
                  commitState.currentCommitId(), tableName, snapshotId, validThroughTs));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, valid-through {}",
          tableName,
          snapshotId,
          commitState.currentCommitId(),
          validThroughTs);
    }
  }

  void terminate() {
    this.terminated = true;

    exec.shutdownNow();

    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }
  }
}
