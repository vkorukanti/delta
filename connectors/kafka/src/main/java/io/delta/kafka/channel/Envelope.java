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

import io.delta.kafka.events.Event;

class Envelope {
  private final Event event;
  private final int partition;
  private final long offset;

  Envelope(Event event, int partition, long offset) {
    this.event = event;
    this.partition = partition;
    this.offset = offset;
  }

  Event event() {
    return event;
  }

  int partition() {
    return partition;
  }

  long offset() {
    return offset;
  }
}
