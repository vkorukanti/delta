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
package io.delta.kafka.data;

import io.delta.kafka.DeltaSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

class DeltaWriterFactory {
  private final DeltaSinkConfig config;

  DeltaWriterFactory(DeltaSinkConfig config) {
    this.config = config;
  }

  RecordWriter createWriter(String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    // TODO: handle new tables
    return new DeltaWriter(config);
  }
}
