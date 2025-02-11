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

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.TableConfig.APPEND_ONLY;
import static io.delta.kernel.internal.TableConfig.COLUMN_MAPPING_MODE;
import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;
import static io.delta.kernel.internal.TableConfig.ROW_TRACKING_ENABLED;
import static io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode.NONE;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.CaseInsensitiveMap;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

/** Contains utility methods related to the Delta table feature support in protocol. */
public class TableFeatures {

  /** Min reader version that supports reader features. */
  public static final int TABLE_FEATURES_MIN_READER_VERSION = 3;

  /** Min reader version that supports writer features. */
  public static final int TABLE_FEATURES_MIN_WRITER_VERSION = 7;

  //////////////////////////////////////////////////////////////////////
  /// Define the {@link TableFeature}s that are supported by Kernel  ///
  //////////////////////////////////////////////////////////////////////
  /** An interface to indicate a feature is legacy, i.e., released before Table Features. */
  public interface LegacyFeatureType extends FeatureAutoEnabledByMetadata {
    @Override
    default boolean automaticallyUpdateProtocolOfExistingTables() {
      return true; // legacy features can always be applied to existing tables
    }
  }

  /** An interface to indicate a feature applies to readers and writers. */
  public interface ReaderWriterFeatureType {}

  /** A base class for all table legacy writer-only features. */
  public abstract static class LegacyWriterFeature extends TableFeature
      implements LegacyFeatureType {
    public LegacyWriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }
  }

  /** A base class for all table legacy reader-writer features. */
  public abstract static class LegacyReaderWriterFeature extends TableFeature
      implements LegacyFeatureType {
    public LegacyReaderWriterFeature(
        String featureName, int minReaderVersion, int minWriterVersion) {
      super(featureName, minReaderVersion, minWriterVersion);
    }
  }

  /** A base class for all non-legacy table writer features. */
  public static class WriterFeature extends TableFeature {
    public WriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }
  }

  /** A base class for all non-legacy table reader-writer features. */
  public static class ReaderWriterFeature extends TableFeature implements ReaderWriterFeatureType {
    public ReaderWriterFeature(String featureName, int minReaderVersion, int minWriterVersion) {
      super(featureName, minReaderVersion, minWriterVersion);
    }
  }

  public static final TableFeature APPEND_ONLY_FEATURE = new AppendOnlyFeature();

  private static class AppendOnlyFeature extends LegacyWriterFeature {
    AppendOnlyFeature() {
      super(/* featureName = */ "appendOnly", /* minWriterVersion = */ 2);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return APPEND_ONLY.fromMetadata(metadata);
    }
  }

  public static final TableFeature INVARIANTS_FEATURE = new InvariantsFeature();

  private static class InvariantsFeature extends LegacyWriterFeature {
    InvariantsFeature() {
      super(/* featureName = */ "invariants", /* minWriterVersion = */ 2);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasInvariants(metadata.getSchema());
    }
  }

  public static final TableFeature COLUMN_MAPPING_FEATURE = new ColumnMappingFeature();

  private static class ColumnMappingFeature extends LegacyReaderWriterFeature {
    ColumnMappingFeature() {
      super(
          /* featureName = */ "columnMapping",
          /* minReaderVersion = */ 2,
          /* minWriterVersion = */ 5);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return COLUMN_MAPPING_MODE.fromMetadata(metadata) != NONE;
    }
  }

  public static final TableFeature VARIANT_FEATURE = new VariantTypeTableFeature("variantType");
  public static final TableFeature VARIANT_PREVIEW_FEATURE =
      new VariantTypeTableFeature("variantType-preview");

  static class VariantTypeTableFeature extends ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    VariantTypeTableFeature(String featureName) {
      super(
          /* featureName = */ featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      // TODO: check schema recursively for variant types
      return false;
    }
  }

  public static final TableFeature DOMAIN_METADATA_FEATURE =
      new WriterFeature("domainMetadata", /* minWriterVersion = */ 7);

  public static final TableFeature ROW_TRACKING_FEATURE = new RowTrackingFeature();

  private static class RowTrackingFeature extends WriterFeature
      implements FeatureAutoEnabledByMetadata {
    RowTrackingFeature() {
      super("rowTracking", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return ROW_TRACKING_ENABLED.fromMetadata(metadata);
    }

    @Override
    public Set<TableFeature> requiredFeatures() {
      return Collections.singleton(DOMAIN_METADATA_FEATURE);
    }
  }

  public static final TableFeature ICEBERG_COMPAT_V2_TABLE_FEATURE =
      new IcebergCompatV2TableFeature();

  private static class IcebergCompatV2TableFeature extends WriterFeature
      implements FeatureAutoEnabledByMetadata {
    IcebergCompatV2TableFeature() {
      super("icebergCompatV2", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    }

    public @Override Set<TableFeature> requiredFeatures() {
      return Collections.singleton(COLUMN_MAPPING_FEATURE);
    }
  }

  public static final TableFeature TYPE_WIDENING_TABLE_FEATURE =
      new TypeWideningTableFeature("typeWidening");
  public static final TableFeature TYPE_WIDENING_PREVIEW_TABLE_FEATURE =
      new TypeWideningTableFeature("typeWidening-preview");

  private static class TypeWideningTableFeature extends ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TypeWideningTableFeature(String featureName) {
      super(featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ENABLE_TYPE_WIDENING.fromMetadata(metadata);
      // TODO: there is more. Need to check if any of the fields
    }
  }

  public static final TableFeature IN_COMMIT_TIMESTAMP_FEATURE =
      new InCommitTimestampTableFeature();

  private static class InCommitTimestampTableFeature extends WriterFeature
      implements FeatureAutoEnabledByMetadata {
    InCommitTimestampTableFeature() {
      super("inCommitTimestamp", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature VACUUM_PROTOCOL_CHECK_TABLE_FEATURE =
      new ReaderWriterFeature(
          "vacuumProtocolCheck", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);

  public static final List<TableFeature> TABLE_FEATURES =
      Arrays.asList(
          APPEND_ONLY_FEATURE,
          INVARIANTS_FEATURE,
          COLUMN_MAPPING_FEATURE,
          VARIANT_FEATURE,
          VARIANT_PREVIEW_FEATURE,
          DOMAIN_METADATA_FEATURE,
          ROW_TRACKING_FEATURE,
          ICEBERG_COMPAT_V2_TABLE_FEATURE,
          TYPE_WIDENING_TABLE_FEATURE,
          TYPE_WIDENING_PREVIEW_TABLE_FEATURE,
          IN_COMMIT_TIMESTAMP_FEATURE,
          VACUUM_PROTOCOL_CHECK_TABLE_FEATURE);

  public static final CaseInsensitiveMap<TableFeature> TABLE_FEATURE_MAP =
      new CaseInsensitiveMap<TableFeature>() {
        {
          for (TableFeature feature : TABLE_FEATURES) {
            put(feature.featureName(), feature);
          }
        }
      };

  /**
   * Does this protocol reader version support specifying the explicit reader feature set in
   * protocol?
   */
  public static boolean supportsReaderFeatures(int minReaderVersion) {
    return minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION;
  }

  /**
   * Does this protocol writer version support specifying the explicit writer feature set in
   * protocol?
   */
  public static boolean supportsWriterFeatures(int minWriterVersion) {
    return minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION;
  }

  /** Returns the minimum reader/writer versions required to support all provided features. */
  public static Tuple2<Integer, Integer> minimumRequiredVersions(Set<TableFeature> features) {
    int minReaderVersion =
        features.stream().mapToInt(TableFeature::minReaderVersion).max().orElse(0);

    int minWriterVersion =
        features.stream().mapToInt(TableFeature::minWriterVersion).max().orElse(0);

    return new Tuple2<>(Math.max(minReaderVersion, 1), Math.max(minWriterVersion, 2));
  }

  /**
   * Upgrade the current protocol to satisfy all auto-update capable features required by the table
   * metadata. A Delta error will be thrown if a non-auto-update capable feature is required by the
   * metadata and not in the resulting protocol, in such a case the user must run `ALTER TABLE` to
   * add support for this feature beforehand using the `delta.feature.featureName` table property.
   *
   * <p>Refer to {@link FeatureAutoEnabledByMetadata#automaticallyUpdateProtocolOfExistingTables()}
   * to know more about "auto-update capable" features.
   *
   * <p>Note: this method only considers metadata-enabled features. To avoid confusion, the caller
   * must apply and remove protocol-related table properties from the metadata before calling this
   * method.
   */
  public Optional<Protocol> upgradeProtocolFromMetadataForExistingTable(
      Metadata metadata, Protocol current) {

    Protocol required =
        new Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
            .withFeatures(extractAutomaticallyEnabledFeatures(metadata, current))
            .normalized();

    if (!required.canUpgradeTo(current)) {
      // When the current protocol does not satisfy metadata requirement, some additional features
      // must be supported by the protocol. We assert those features can actually perform the
      // auto-update.
      assertMetadataTableFeaturesAutomaticallySupported(
          current.getImplicitAndExplicitlyEnabledFeatures(),
          required.getImplicitAndExplicitlyEnabledFeatures());
      return Optional.of(required.merge(current));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Extracts all table features that are enabled by the given metadata and the optional protocol.
   * This includes all already enabled features (if a protocol is provided), the features enabled
   * directly by metadata, and all of their (transitive) dependencies.
   */
  public Set<TableFeature> extractAutomaticallyEnabledFeatures(
      Metadata metadata, Protocol protocol) {
    Set<TableFeature> protocolEnabledFeatures =
        protocol.getWriterFeatures().stream()
            .map(TABLE_FEATURE_MAP::get)
            .map(
                f -> {
                  if (f == null) {
                    throw new IllegalArgumentException("Unknown feature in protocol: " + f);
                  }
                  return f;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

    Set<TableFeature> metadataEnabledFeatures =
        TableFeatures.TABLE_FEATURES.stream()
            .filter(f -> f instanceof FeatureAutoEnabledByMetadata)
            .filter(
                f ->
                    ((FeatureAutoEnabledByMetadata) f)
                        .metadataRequiresFeatureToBeEnabled(protocol, metadata))
            .collect(Collectors.toSet());

    Set<TableFeature> combinedFeatures = new HashSet<>(protocolEnabledFeatures);
    combinedFeatures.addAll(metadataEnabledFeatures);

    // Qn for Paddy: Why do we need to add the dependencies here and also in `Protocol.withFeature`
    return getDependencyClosure(combinedFeatures);
  }

  /**
   * Returns the smallest set of table features that contains `features` and that also contains all
   * dependencies of all features in the returned set.
   */
  private Set<TableFeature> getDependencyClosure(Set<TableFeature> features) {
    Set<TableFeature> requiredFeatures = new HashSet<>(features);
    features.forEach(feature -> requiredFeatures.addAll(feature.requiredFeatures()));

    if (features.equals(requiredFeatures)) {
      return features;
    } else {
      return getDependencyClosure(requiredFeatures);
    }
  }

  /**
   * Ensure all features listed in `currentFeatures` are also listed in `requiredFeatures`, or, if
   * one is not listed, it must be capable to auto-update a protocol.
   *
   * <p>Refer to FeatureAutomaticallyEnabledByMetadata.automaticallyUpdateProtocolOfExistingTables
   * to know more about "auto-update capable" features.
   *
   * <p>Note: Caller must make sure `requiredFeatures` is obtained from a min protocol that
   * satisfies a table metadata.
   */
  private void assertMetadataTableFeaturesAutomaticallySupported(
      Set<TableFeature> currentFeatures, Set<TableFeature> requiredFeatures) {

    Set<TableFeature> newFeatures = new HashSet<>(requiredFeatures);
    newFeatures.removeAll(currentFeatures);

    Set<FeatureAutoEnabledByMetadata> autoUpdateCapableFeatures = new HashSet<>();
    Set<FeatureAutoEnabledByMetadata> nonAutoUpdateCapableFeatures = new HashSet<>();

    for (TableFeature feature : newFeatures) {
      if (feature instanceof FeatureAutoEnabledByMetadata) {
        FeatureAutoEnabledByMetadata autoFeature = (FeatureAutoEnabledByMetadata) feature;
        if (autoFeature.automaticallyUpdateProtocolOfExistingTables()) {
          autoUpdateCapableFeatures.add(autoFeature);
        } else {
          nonAutoUpdateCapableFeatures.add(autoFeature);
        }
      }
    }

    if (!nonAutoUpdateCapableFeatures.isEmpty()) {
      // The "current features" we give to the user are from the original protocol, plus
      // features newly supported by table properties in the current transaction, plus
      // metadata-enabled features that are auto-update capable. The first two are provided by
      // `currentFeatures`.
      Set<TableFeature> allCurrentFeatures = new HashSet<>(currentFeatures);
      allCurrentFeatures.addAll(
          autoUpdateCapableFeatures.stream()
              .map(f -> (TableFeature) f)
              .collect(Collectors.toSet()));

      // TODO: fix this error message to be more informative
      throw new UnsupportedOperationException(
          "The current protocol does not support the following features: "
              + nonAutoUpdateCapableFeatures
              + ". The protocol must be updated to support these features. "
              + "The current protocol supports the following features: "
              + allCurrentFeatures);
    }
  }

  private static final Set<String> SUPPORTED_WRITER_FEATURES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("appendOnly");
              add("inCommitTimestamp");
              add("columnMapping");
              add("typeWidening-preview");
              add("typeWidening");
              add("domainMetadata");
              add("rowTracking");
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

  ////////////////////
  // Helper Methods //
  ////////////////////
  public static void validateReadSupportedTable(
      Protocol protocol, String tablePath, Optional<Metadata> metadata) {
    switch (protocol.getMinReaderVersion()) {
      case 1:
        break;
      case 2:
        metadata.ifPresent(ColumnMapping::throwOnUnsupportedColumnMappingMode);
        break;
      case 3:
        Set<String> readerFeatures = protocol.getReaderFeatures();
        if (!SUPPORTED_READER_FEATURES.containsAll(readerFeatures)) {
          Set<String> unsupportedFeatures = new HashSet<>(readerFeatures);
          unsupportedFeatures.removeAll(SUPPORTED_READER_FEATURES);
          throw DeltaErrors.unsupportedReaderFeature(tablePath, unsupportedFeatures);
        }
        if (readerFeatures.contains("columnMapping")) {
          metadata.ifPresent(ColumnMapping::throwOnUnsupportedColumnMappingMode);
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
   * @param tableSchema Table schema
   */
  public static void validateWriteSupportedTable(
      Protocol protocol, Metadata metadata, StructType tableSchema, String tablePath) {
    int minWriterVersion = protocol.getMinWriterVersion();
    switch (minWriterVersion) {
      case 1:
        break;
      case 2:
        // Append-only and column invariants are the writer features added in version 2
        // Append-only is supported, but not the invariants
        validateNoInvariants(tableSchema);
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
          if (writerFeature.equals("invariants")) {
            // For version 7, we allow 'invariants' to be present in the protocol's writerFeatures
            // to unblock certain use cases, provided that no invariants are defined in the schema.
            validateNoInvariants(tableSchema);
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
    return isWriterFeatureSupported(protocol, "domainMetadata");
  }

  /**
   * Check if the table protocol supports the "rowTracking" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the protocol supports row tracking, false otherwise
   */
  public static boolean isRowTrackingSupported(Protocol protocol) {
    return isWriterFeatureSupported(protocol, "rowTracking");
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

  private static void validateNoInvariants(StructType tableSchema) {
    if (hasInvariants(tableSchema)) {
      throw columnInvariantsNotSupported();
    }
  }

  private static boolean hasInvariants(StructType tableSchema) {
    return tableSchema.fields().stream()
        .anyMatch(field -> field.getMetadata().contains("delta.invariants"));
  }

  private static boolean hasCheckConstraints(Metadata metadata) {
    return metadata.getConfiguration().entrySet().stream()
        .findAny()
        .map(entry -> entry.getKey().equals("delta.constraints."))
        .orElse(false);
  }

  private static boolean isWriterFeatureSupported(Protocol protocol, String featureName) {
    Set<String> writerFeatures = protocol.getWriterFeatures();
    if (writerFeatures == null) {
      return false;
    }
    return writerFeatures.contains(featureName) && protocol.getMinWriterVersion() >= 7;
  }
}
