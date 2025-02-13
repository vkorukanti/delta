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

package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

/** Contains utility methods related to the Delta table feature support in protocol. */
public class TableFeatures {

  private static final Set<String> SUPPORTED_WRITER_FEATURES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("appendOnly");
              add("inCommitTimestamp");
              add("columnMapping");
              add("typeWidening-preview");
              add("typeWidening");
              add(DOMAIN_METADATA_FEATURE_NAME);
              add(ROW_TRACKING_FEATURE_NAME);
            }
          });

  private static final Set<String> SUPPORTED_READER_FEATURES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("columnMapping");
              add("deletionVectors");
              add("timestampNtz");
              add("typeWidening-preview");
              add("typeWidening");
              add("vacuumProtocolCheck");
              add("variantType");
              add("variantType-preview");
              add("v2Checkpoint");
            }
          });

  public static final String DOMAIN_METADATA_FEATURE_NAME = "domainMetadata";

  public static final String ROW_TRACKING_FEATURE_NAME = "rowTracking";

  public static final String INVARIANTS_FEATURE_NAME = "invariants";

  /** The minimum writer version required to support table features. */
  public static final int TABLE_FEATURES_MIN_WRITER_VERSION = 7;

  ////////////////////
  // Helper Methods //
  ////////////////////

  public static void validateReadSupportedTable(Protocol protocol, String tablePath) {
    switch (protocol.getMinReaderVersion()) {
      case 1:
        break;
      case 2:
        break;
      case 3:
        List<String> readerFeatures = protocol.getReaderFeatures();
        if (!SUPPORTED_READER_FEATURES.containsAll(readerFeatures)) {
          Set<String> unsupportedFeatures = new HashSet<>(readerFeatures);
          unsupportedFeatures.removeAll(SUPPORTED_READER_FEATURES);
          throw DeltaErrors.unsupportedReaderFeature(tablePath, unsupportedFeatures);
        }
        break;
      default:
        throw DeltaErrors.unsupportedReaderProtocol(tablePath, protocol.getMinReaderVersion());
    }
  }

  /**
   * Utility method to validate whether the given table is supported for writing from Kernel.
   * Currently, the support is as follows:
   *
   * <ul>
   *   <li>protocol writer version 1.
   *   <li>protocol writer version 2 only with appendOnly feature enabled.
   *   <li>protocol writer version 7 with {@code appendOnly}, {@code inCommitTimestamp}, {@code
   *       columnMapping}, {@code typeWidening}, {@code domainMetadata}, {@code rowTracking} feature
   *       enabled.
   * </ul>
   *
   * @param protocol Table protocol
   * @param metadata Table metadata
   */
  public static void validateWriteSupportedTable(
      Protocol protocol, Metadata metadata, String tablePath) {
    int minWriterVersion = protocol.getMinWriterVersion();
    switch (minWriterVersion) {
      case 1:
        break;
      case 2:
        // Append-only and column invariants are the writer features added in version 2
        // Append-only is supported, but not the invariants
        validateNoInvariants(metadata.getSchema());
        break;
      case 3:
        // Check constraints are added in version 3
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 4:
        // CDF and generated columns are writer features added in version 4
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 5:
        // Column mapping is the only one writer feature added in version 5
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 6:
        // Identity is the only one writer feature added in version 6
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 7:
        for (String writerFeature : protocol.getWriterFeatures()) {
          if (writerFeature.equals(INVARIANTS_FEATURE_NAME)) {
            // For version 7, we allow 'invariants' to be present in the protocol's writerFeatures
            // to unblock certain use cases, provided that no invariants are defined in the schema.
            validateNoInvariants(metadata.getSchema());
          } else if (!SUPPORTED_WRITER_FEATURES.contains(writerFeature)) {
            throw unsupportedWriterFeature(tablePath, writerFeature);
          }
        }

        // Eventually we may have a way to declare and enforce dependencies between features.
        // By putting this check for row tracking here, it makes it easier to spot that row
        // tracking defines such a dependency that can be implicitly checked.
        if (isRowTrackingSupported(protocol) && !isDomainMetadataSupported(protocol)) {
          throw DeltaErrors.rowTrackingSupportedWithDomainMetadataUnsupported();
        }
        break;
      default:
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
    }
  }

  /**
   * Given the automatically enabled features from Delta table metadata, returns the minimum
   * required reader and writer version that satisfies all enabled table features in the metadata.
   *
   * @param enabledFeatures the automatically enabled features from the Delta table metadata
   * @return the minimum required reader and writer version that satisfies all enabled table
   */
  public static Tuple2<Integer, Integer> minProtocolVersionFromAutomaticallyEnabledFeatures(
      Set<String> enabledFeatures) {

    int readerVersion = 0;
    int writerVersion = 0;

    for (String feature : enabledFeatures) {
      readerVersion = Math.max(readerVersion, getMinReaderVersion(feature));
      writerVersion = Math.max(writerVersion, getMinWriterVersion(feature));
    }

    return new Tuple2<>(readerVersion, writerVersion);
  }

  /**
   * Extract the writer features that should be enabled automatically based on the metadata which
   * are not already enabled. For example, the {@code inCommitTimestamp} feature should be enabled
   * when the delta property name (delta.enableInCommitTimestamps) is set to true in the metadata if
   * it is not already enabled.
   *
   * @param metadata the metadata of the table
   * @param protocol the protocol of the table
   * @return the writer features that should be enabled automatically
   */
  public static Set<String> extractAutomaticallyEnabledWriterFeatures(
      Metadata metadata, Protocol protocol) {
    return TableFeatures.SUPPORTED_WRITER_FEATURES.stream()
        .filter(f -> metadataRequiresWriterFeatureToBeEnabled(metadata, f))
        .filter(
            f -> protocol.getWriterFeatures() == null || !protocol.getWriterFeatures().contains(f))
        .collect(Collectors.toSet());
  }

  /**
   * Checks if the table protocol supports the "domainMetadata" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the "domainMetadata" feature is supported, false otherwise
   */
  public static boolean isDomainMetadataSupported(Protocol protocol) {
    return isWriterFeatureSupported(protocol, DOMAIN_METADATA_FEATURE_NAME);
  }

  /**
   * Check if the table protocol supports the "rowTracking" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the protocol supports row tracking, false otherwise
   */
  public static boolean isRowTrackingSupported(Protocol protocol) {
    return isWriterFeatureSupported(protocol, ROW_TRACKING_FEATURE_NAME);
  }

  /**
   * Get the minimum reader version required for a feature.
   *
   * @param feature the feature
   * @return the minimum reader version required for the feature
   */
  private static int getMinReaderVersion(String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return 3;
      default:
        return 1;
    }
  }

  /**
   * Get the minimum writer version required for a feature.
   *
   * @param feature the feature
   * @return the minimum writer version required for the feature
   */
  private static int getMinWriterVersion(String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return 7;
      default:
        return 2;
    }
  }

  /**
   * Determine whether a writer feature must be supported and enabled to satisfy the metadata
   * requirements.
   *
   * @param metadata the table metadata
   * @param feature the writer feature to check
   * @return whether the writer feature must be enabled
   */
  private static boolean metadataRequiresWriterFeatureToBeEnabled(
      Metadata metadata, String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata);
      default:
        return false;
    }
  }

  private static boolean isWriterFeatureSupported(Protocol protocol, String featureName) {
    List<String> writerFeatures = protocol.getWriterFeatures();
    if (writerFeatures == null) {
      return false;
    }
    return writerFeatures.contains(featureName)
        && protocol.getMinWriterVersion() >= TABLE_FEATURES_MIN_WRITER_VERSION;
  }

  private static void validateNoInvariants(StructType tableSchema) {
    if (hasInvariants(tableSchema)) {
      throw DeltaErrors.columnInvariantsNotSupported();
    }
  }

  static boolean hasInvariants(StructType tableSchema) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ false, // constraints are not allowed in maps or
            // arrays
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.invariants"))
        .isEmpty();
  }

  /**
   * Check if the table schema has a column of type. Caution: works only for the primitive types.
   */
  static boolean hasTypeColumn(StructType tableSchema, DataType type) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ true,
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getDataType().equals(type))
        .isEmpty();
  }
}
