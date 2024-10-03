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

import io.delta.kafka.Committer;
import io.delta.kafka.DeltaSinkConfig;
import io.delta.kafka.data.SinkWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class CommitterImpl implements Committer {
  private CoordinatorThread coordinatorThread;
  private Worker worker;

  static class TopicPartitionComparator implements Comparator<TopicPartition> {

    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
      int result = o1.topic().compareTo(o2.topic());
      if (result == 0) {
        result = Integer.compare(o1.partition(), o2.partition());
      }
      return result;
    }
  }

  @Override
  public void start(DeltaSinkConfig config, SinkTaskContext context) {
    KafkaClientFactory clientFactory = new KafkaClientFactory(config.kafkaProps());

    ConsumerGroupDescription groupDesc;
    try (Admin admin = clientFactory.createAdmin()) {
      groupDesc = KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin);
    }

    if (groupDesc.state() == ConsumerGroupState.STABLE) {
      Collection<MemberDescription> members = groupDesc.members();
      Set<TopicPartition> partitions = context.assignment();
      if (isLeader(members, partitions)) {
        Coordinator coordinator = new Coordinator(config, members, clientFactory, context);
        coordinatorThread = new CoordinatorThread(coordinator);
        coordinatorThread.start();
      }
    }

    SinkWriter sinkWriter = new SinkWriter(config);
    worker = new Worker(config, clientFactory, sinkWriter, context);
    worker.start();
  }

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      worker.save(sinkRecords);
    }
    processControlEvents();
  }

  @Override
  public void stop() {
    if (worker != null) {
      worker.stop();
      worker = null;
    }

    if (coordinatorThread != null) {
      coordinatorThread.terminate();
      coordinatorThread = null;
    }
  }

  boolean isLeader(Collection<MemberDescription> members, Collection<TopicPartition> partitions) {
    // there should only be one task assigned partition 0 of the first topic,
    // so elect that one the leader
    TopicPartition firstTopicPartition =
        members.stream()
            .flatMap(member -> member.assignment().topicPartitions().stream())
            .min(new TopicPartitionComparator())
            .orElseThrow(
                () -> new ConnectException("No partitions assigned, cannot determine leader"));

    return partitions.contains(firstTopicPartition);
  }

  private void processControlEvents() {
    if (coordinatorThread != null && coordinatorThread.isTerminated()) {
      throw new RuntimeException("Coordinator unexpectedly terminated");
    }
    if (worker != null) {
      worker.process();
    }
  }
}
