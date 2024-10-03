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
package io.delta.kafka;

import io.delta.kafka.channel.CommitterImpl;
import io.delta.kernel.internal.util.Preconditions;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class DeltaSinkTask extends SinkTask {
  private DeltaSinkConfig config;
  private Committer committer;

  @Override
  public String version() {
    return DeltaSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new DeltaSinkConfig(props);
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    Preconditions.checkArgument(committer == null, "Committer already open");

    committer = new CommitterImpl();
    committer.start(config, context);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    close();
  }

  private void close() {
    if (committer != null) {
      committer.stop();
      committer = null;
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (committer != null) {
      committer.save(sinkRecords);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (committer != null) {
      committer.save(null);
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // offset commit is handled by the worker
    return ImmutableMap.of();
  }

  @Override
  public void stop() {
    close();
  }
}
