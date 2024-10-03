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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class DeltaSinkConnector extends SinkConnector {
  private Map<String, String> props;

  @Override
  public String version() {
    return DeltaSinkConfig.version();
  }

  @Override
  public void start(Map<String, String> connectorProps) {
    this.props = connectorProps;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DeltaSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String txnSuffix = "-txn-" + UUID.randomUUID() + "-";
    return IntStream.range(0, maxTasks)
        .mapToObj(
            i -> {
              Map<String, String> map = Maps.newHashMap(props);
              map.put(DeltaSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP, txnSuffix + i);
              return map;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return DeltaSinkConfig.CONFIG_DEF;
  }
}
