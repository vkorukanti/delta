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
import io.delta.kafka.data.Offset;
import io.delta.kafka.data.SinkWriter;
import io.delta.kafka.data.SinkWriterResult;
import io.delta.kafka.events.DataComplete;
import io.delta.kafka.events.DataWritten;
import io.delta.kafka.events.Event;
import io.delta.kafka.events.PayloadType;
import io.delta.kafka.events.StartCommit;
import io.delta.kafka.events.TopicPartitionOffset;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

class Worker extends Channel {

  private final DeltaSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;

  Worker(
      DeltaSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        DeltaSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
  }

  void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(commitId, writeResult.dataFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());

    return true;
  }

  @Override
  void stop() {
    super.stop();
    sinkWriter.close();
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
