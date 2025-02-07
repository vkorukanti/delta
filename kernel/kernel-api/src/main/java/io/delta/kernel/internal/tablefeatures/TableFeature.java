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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import java.util.Set;

/**
 * Base class for table features.
 *
 * <p>A feature can be <b>explicitly supported</b> by a table's protocol when the protocol contains
 * a feature's `name`. Writers (for writer-only features) or readers and writers (for reader-writer
 * features) must recognize supported features and must handle them appropriately.
 *
 * <p>A table feature that released before Delta Table Features (reader version 3 and writer version
 * 7) is considered as a <strong>legacy feature</strong>. Legacy features are <strong> implicitly
 * supported</strong> when (a) the protocol does not support table features, i.e., has reader
 * version less than 3 or writer version less than 7 and (b) the feature's minimum reader/writer
 * version is less than or equal to the current protocol's reader/writer version.
 *
 * <p>Separately, a feature can be automatically supported by a table's metadata when certain
 * feature-specific table properties are set. For example, `changeDataFeed` is automatically
 * supported when there's a table property `delta.enableChangeDataFeed=true`. This is independent of
 * the table's enabled features. When a feature is supported (explicitly or implicitly) by the table
 * protocol but its metadata requirements are not satisfied, then clients still have to understand
 * the feature (at least to the extent that they can read and preserve the existing data in the
 * table that uses the feature).
 */
public interface TableFeature {

  /** @return the name of the table feature. */
  String featureName();

  /**
   * @return true if this feature is applicable to both reader and writer, false if it is
   *     writer-only.
   */
  boolean isReaderWriterFeature();

  /** @return the minimum reader version this feature requires */
  int minReaderVersion();

  /** @return the minimum writer version that this feature requires. */
  int minWriterVersion();

  /** @return if this feature is a legacy feature? */
  boolean isLegacyFeature();

  /**
   * Set of table features that this table feature depends on. I.e. the set of features that need to
   * be enabled if this table feature is enabled.
   *
   * @return the set of table features that this table feature depends on.
   */
  Set<TableFeature> requiredFeatures();

  /**
   * Validate the table feature. This method should throw an exception if the table feature
   * properties are invalid. Should be called after the object deriving the {@link TableFeature} is
   * constructed.
   */
  default void validate() {
    if (!isReaderWriterFeature()) {
      checkArgument(minReaderVersion() == 0, "Writer-only feature must have minReaderVersion=0");
    }

    if (isLegacyFeature()) {
      checkArgument(
          this instanceof FeatureAutoEnabledByMetadata
              && ((FeatureAutoEnabledByMetadata) this)
                  .automaticallyUpdateProtocolOfExistingTables(),
          "Legacy feature must be auto-enable capable based on metadata");
    }
  }
}
