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
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.CaseInsensitiveMap;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StructType;
import java.util.*;

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
      implements ReaderWriterFeatureType, LegacyFeatureType {
    public LegacyWriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
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
  public abstract static class WriterFeature extends TableFeature {
    public WriterFeature(String featureName, int minWriterVersion) {
      super(featureName, /* minReaderVersion = */ 0, minWriterVersion);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }
  }

  /** A base class for all non-legacy table reader-writer features. */
  public abstract static class ReaderWriterFeature extends TableFeature
      implements ReaderWriterFeatureType {
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

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
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

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // If there is no invariant, then the table is supported
      return !hasInvariants(metadata.getSchema());
    }
  }

  public static final TableFeature CONSTRAINTS_FEATURE = new ConstraintsFeature();

  private static class ConstraintsFeature extends LegacyWriterFeature {
    ConstraintsFeature() {
      super("constraints", /* minWriterVersion = */ 3);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // If there is no check constraint, then the table is supported
      return hasCheckConstraints(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasCheckConstraints(metadata);
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

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final TableFeature IDENTITY_COLUMNS_FEATURE = new IdentityColumnsFeature();

  private static class IdentityColumnsFeature extends LegacyWriterFeature {
    IdentityColumnsFeature() {
      super("identity", /* minWriterVersion = */ 6);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return !hasIdentityColumns(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasIdentityColumns(metadata);
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

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }
  }

  public static final TableFeature DOMAIN_METADATA_FEATURE = new DomainMetadataFeature();

  private static class DomainMetadataFeature extends WriterFeature {
    DomainMetadataFeature() {
      super("domainMetadata", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

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

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final TableFeature DELETION_VECTORS_FEATURE = new DeletionVectorsTableFeature();

  private static class DeletionVectorsTableFeature extends ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    DeletionVectorsTableFeature() {
      super("deletionVectors", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // We currently only support blind appends. So we don't need to do anything special for
      // writing
      // into a table with deletion vectors enabled.
      return true;
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ENABLE_DELETION_VECTORS_CREATION.fromMetadata(metadata);
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

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
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

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to support it.
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

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final TableFeature TIMESTAMP_NTZ_FEATURE = new TimestampNtzTableFeature();

  private static class TimestampNtzTableFeature extends ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TimestampNtzTableFeature() {
      super("timestampNtz", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return false; // we can't allow feature upgrade without manually adding the feature
      // through `delta.feature.timestampNtz = supported` table property.
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      // TODO: check schema recursively for timestamp_ntz types
      return false;
    }
  }

  public static final TableFeature CHECKPOINT_V2_FEATURE = new CheckpointV2TableFeature();

  private static class CheckpointV2TableFeature extends ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    CheckpointV2TableFeature() {
      super("v2Checkpoint", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true; // In order to commit, there is no extra work required. This affects
      // the checkpoint format only. When v2 is enabled, writing classic checkpoints is
      // still allowed.
    }

    @Override
    public boolean automaticallyUpdateProtocolOfExistingTables() {
      return true;
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      // TODO: define an enum for checkpoint policy when we start supporting writing v2 checkpoints
      return "v2".equals(TableConfig.CHECKPOINT_POLICY.fromMetadata(metadata));
    }
  }

  public static final TableFeature VACUUM_PROTOCOL_CHECK_FEATURE =
      new VacuumProtocolCheckTableFeature();

  private static class VacuumProtocolCheckTableFeature extends ReaderWriterFeature {
    VacuumProtocolCheckTableFeature() {
      super("vacuumProtocolCheck", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelReadSupport() {
      return true;
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final List<TableFeature> TABLE_FEATURES =
      Arrays.asList(
          APPEND_ONLY_FEATURE,
          INVARIANTS_FEATURE,
          COLUMN_MAPPING_FEATURE,
          VARIANT_FEATURE,
          VARIANT_PREVIEW_FEATURE,
          DOMAIN_METADATA_FEATURE,
          ROW_TRACKING_FEATURE,
          IDENTITY_COLUMNS_FEATURE,
          CONSTRAINTS_FEATURE,
          ICEBERG_COMPAT_V2_TABLE_FEATURE,
          TYPE_WIDENING_TABLE_FEATURE,
          TYPE_WIDENING_PREVIEW_TABLE_FEATURE,
          IN_COMMIT_TIMESTAMP_FEATURE,
          TIMESTAMP_NTZ_FEATURE,
          VACUUM_PROTOCOL_CHECK_FEATURE,
          DELETION_VECTORS_FEATURE,
          CHECKPOINT_V2_FEATURE);

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
          current.getImplicitlyAndExplicitlyEnabledFeatures(),
          required.getImplicitlyAndExplicitlyEnabledFeatures());
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
  public static Set<TableFeature> extractAutomaticallyEnabledFeatures(
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
            .filter(Objects::nonNull) // TODO: throw error if we can't find them
            .collect(toSet());

    Set<TableFeature> metadataEnabledFeatures =
        TableFeatures.TABLE_FEATURES.stream()
            .filter(f -> f instanceof FeatureAutoEnabledByMetadata)
            .filter(
                f ->
                    ((FeatureAutoEnabledByMetadata) f)
                        .metadataRequiresFeatureToBeEnabled(protocol, metadata))
            .collect(toSet());

    Set<TableFeature> combinedFeatures = new HashSet<>(protocolEnabledFeatures);
    combinedFeatures.addAll(metadataEnabledFeatures);

    // Qn for Paddy: Why do we need to add the dependencies here and also in `Protocol.withFeature`
    return getDependencyClosure(combinedFeatures);
  }

  /**
   * Returns the smallest set of table features that contains `features` and that also contains all
   * dependencies of all features in the returned set.
   */
  private static Set<TableFeature> getDependencyClosure(Set<TableFeature> features) {
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
          autoUpdateCapableFeatures.stream().map(f -> (TableFeature) f).collect(toSet()));

      // TODO: fix this error message to be more informative
      throw new UnsupportedOperationException(
          "The current protocol does not support the following features: "
              + nonAutoUpdateCapableFeatures
              + ". The protocol must be updated to support these features. "
              + "The current protocol supports the following features: "
              + allCurrentFeatures);
    }
  }

  ////////////////////
  // Helper Methods //
  /// /////////////////
  public static void validateReadSupportedTable(Protocol protocol, String tablePath) {
    Set<TableFeature> unsupportedFeatures =
        protocol.getImplicitlyAndExplicitlyEnabledFeatures().stream()
            .filter(f -> !f.hasKernelReadSupport())
            .collect(toSet());

    if (!unsupportedFeatures.isEmpty()) {
      throw unsupportedReaderFeature(
          tablePath, unsupportedFeatures.stream().map(TableFeature::featureName).collect(toSet()));
    }
  }

  /**
   * Utility method to validate whether the given table is supported for writing from Kernel.
   *
   * @param protocol Table protocol
   * @param metadata Table metadata
   */
  public static void validateWriteSupportedTable(
      Protocol protocol, Metadata metadata, String tablePath) {
    // TODO: should we check the table is writer and reader support?
    protocol
        .getImplicitlyAndExplicitlyEnabledFeatures()
        .forEach(
            f -> {
              if (!f.hasKernelWriteSupport(metadata)) {
                throw unsupportedWriterFeature(tablePath, f.featureName());
              }
            });
  }

  /** Checks if the table protocol supports the "domainMetadata" writer feature. */
  public static boolean isDomainMetadataSupported(Protocol protocol) {
    return protocol.getImplicitlyAndExplicitlyEnabledFeatures().contains(DOMAIN_METADATA_FEATURE);
  }

  /** Check if the table protocol supports the "rowTracking" writer feature. */
  public static boolean isRowTrackingSupported(Protocol protocol) {
    return protocol.getImplicitlyAndExplicitlyEnabledFeatures().contains(ROW_TRACKING_FEATURE);
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

  private static boolean hasIdentityColumns(Metadata metadata) {
    return false; // TODO: implement.
  }
}
