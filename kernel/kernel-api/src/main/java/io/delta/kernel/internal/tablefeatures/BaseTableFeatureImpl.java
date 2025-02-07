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
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.tablefeatures.TableFeatures.LegacyFeatureType;
import io.delta.kernel.internal.tablefeatures.TableFeatures.ReaderWriterFeatureType;
import java.util.Collections;
import java.util.Set;

/** Base class for table features. See {@link TableFeature} for more information. */
public abstract class BaseTableFeatureImpl implements TableFeature {
  private final String featureName;
  private final int minReaderVersion;
  private final int minWriterVersion;

  /**
   * Constructor. Does validations to make sure:
   *
   * <ul>
   *   <li>Feature name is not null or empty and has valid characters
   *   <li>minReaderVersion is always 0 for writer features
   *   <li>all legacy features can be auto applied based on the metadata and protocol
   *
   * @param featureName a globally-unique string indicator to represent the feature. All characters
   *     must be letters (a-z, A-Z), digits (0-9), '-', or '_'. Words must be in camelCase.
   * @param minReaderVersion the minimum reader version this feature requires. For a feature that
   *     can only be explicitly supported, this is either `0` or `3` (the reader protocol version
   *     that supports table features), depending on the feature is writer-only or reader-writer.
   *     For a legacy feature that can be implicitly supported, this is the first protocol version
   *     which the feature is introduced.
   * @param minWriterVersion the minimum writer version this feature requires. For a feature that
   *     can only be explicitly supported, this is the writer protocol `7` that supports table
   *     features. For a legacy feature that can be implicitly supported, this is the first protocol
   *     version which the feature is introduced.
   */
  public BaseTableFeatureImpl(String featureName, int minReaderVersion, int minWriterVersion) {
    this.featureName = requireNonNull(featureName, "name is null");
    checkArgument(!featureName.isEmpty(), "name is empty");
    checkArgument(
        featureName.chars().allMatch(c -> Character.isLetterOrDigit(c) || c == '-' || c == '_'),
        "name contains invalid characters: " + featureName);
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;

    validate();
  }

  @Override
  public String featureName() {
    return featureName;
  }

  @Override
  public boolean isReaderWriterFeature() {
    return this instanceof ReaderWriterFeatureType;
  }

  @Override
  public int minReaderVersion() {
    return minReaderVersion;
  }

  public int minWriterVersion() {
    return minWriterVersion;
  }

  public boolean isLegacyFeature() {
    return this instanceof LegacyFeatureType;
  }

  @Override
  public Set<TableFeature> requiredFeatures() {
    return Collections.emptySet();
  }
}
