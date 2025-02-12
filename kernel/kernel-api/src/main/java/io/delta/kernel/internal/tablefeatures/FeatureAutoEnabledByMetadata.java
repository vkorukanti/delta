/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.tablefeatures;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;

/**
 * Defines behavior for {@link TableFeature} that can be automatically enabled via a change in a
 * table's metadata, e.g., through setting particular values of certain feature-specific table
 * properties.
 *
 * <p>When the feature's metadata requirements are satisfied for <strong>new tables</strong>, or for
 * <strong>existing tables when {@link #automaticallyUpdateProtocolOfExistingTables()} set to
 * `true`</strong>, the client will silently add the feature to the protocol's `readerFeatures`
 * and/or `writerFeatures`. Otherwise, a proper protocol version bump must be present in the same
 * transaction.
 */
public interface FeatureAutoEnabledByMetadata {
  /**
   * Whether the feature can automatically update the protocol of an existing table when the
   * metadata requirements are satisfied. As a rule of thumb, a table feature that requires explicit
   * operations (e.g., turning on a table property) should set this flag to `true`, while features
   * that are used implicitly (e.g., when using a new data type) should set this flag to `false`.
   */
  boolean automaticallyUpdateProtocolOfExistingTables();

  /**
   * Determine whether the feature must be supported and enabled because its metadata requirements
   * are satisfied.
   *
   * @param protocol the protocol of the table for features that are already enabled.
   * @param metadata the metadata of the table for properties that can enable the feature.
   */
  boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata);
}
