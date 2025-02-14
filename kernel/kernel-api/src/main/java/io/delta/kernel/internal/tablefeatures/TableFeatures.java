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
import static io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode.NONE;
import static io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ;
import static io.delta.kernel.types.VariantType.VARIANT;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.CaseInsensitiveMap;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructType;
import java.util.*;

/** Contains utility methods related to the Delta table feature support in protocol. */
public class TableFeatures {

  /////////////////////////////////////////////////////////////////////////////////
  /// START: Define the {@link TableFeature}s                                   ///
  /// If feature instance variable ends with                                    ///
  ///  1) `_W_FEATURE` it is a writer only feature.                             ///
  ///  2) `_RW_FEATURE` it is a reader-writer feature.                          ///
  /////////////////////////////////////////////////////////////////////////////////
  public static final TableFeature APPEND_ONLY_W_FEATURE = new AppendOnlyFeature();

  private static class AppendOnlyFeature extends TableFeature.LegacyWriterFeature {
    AppendOnlyFeature() {
      super(/* featureName = */ "appendOnly", /* minWriterVersion = */ 2);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.APPEND_ONLY_ENABLED.fromMetadata(metadata);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final TableFeature INVARIANTS_W_FEATURE = new InvariantsFeature();

  private static class InvariantsFeature extends TableFeature.LegacyWriterFeature {
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

  public static final TableFeature CONSTRAINTS_W_FEATURE = new ConstraintsFeature();

  private static class ConstraintsFeature extends TableFeature.LegacyWriterFeature {
    ConstraintsFeature() {
      super("checkConstraints", /* minWriterVersion = */ 3);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // Kernel doesn't support table with constraints.
      return !hasCheckConstraints(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasCheckConstraints(metadata);
    }
  }

  public static final TableFeature CHANGE_DATA_FEED_W_FEATURE = new ChangeDataFeedFeature();

  private static class ChangeDataFeedFeature extends TableFeature.LegacyWriterFeature {
    ChangeDataFeedFeature() {
      super("changeDataFeed", /* minWriterVersion = */ 4);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.CHANGE_DATA_FEED_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature COLUMN_MAPPING_RW_FEATURE = new ColumnMappingFeature();

  private static class ColumnMappingFeature extends TableFeature.LegacyReaderWriterFeature {
    ColumnMappingFeature() {
      super("columnMapping", /*minReaderVersion = */ 2, /* minWriterVersion = */ 5);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.COLUMN_MAPPING_MODE.fromMetadata(metadata) != NONE;
    }
  }

  public static final TableFeature GENERATED_COLUMNS_W_FEATURE = new GeneratedColumnsFeature();

  private static class GeneratedColumnsFeature extends TableFeature.LegacyWriterFeature {
    GeneratedColumnsFeature() {
      super("generatedColumns", /* minWriterVersion = */ 4);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // Kernel can write as long as there are no generated columns defined
      return !hasGeneratedColumns(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasGeneratedColumns(metadata);
    }
  }

  public static final TableFeature IDENTITY_COLUMNS_W_FEATURE = new IdentityColumnsFeature();

  private static class IdentityColumnsFeature extends TableFeature.LegacyWriterFeature {
    IdentityColumnsFeature() {
      super("identityColumns", /* minWriterVersion = */ 6);
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

  public static final TableFeature VARIANT_RW_FEATURE = new VariantTypeTableFeature("variantType");
  public static final TableFeature VARIANT_RW_PREVIEW_FEATURE =
      new VariantTypeTableFeature("variantType-preview");

  private static class VariantTypeTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    VariantTypeTableFeature(String featureName) {
      super(
          /* featureName = */ featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasTypeColumn(metadata.getSchema(), VARIANT);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }
  }

  public static final TableFeature DOMAIN_METADATA_W_FEATURE = new DomainMetadataFeature();

  private static class DomainMetadataFeature extends TableFeature.WriterFeature {
    DomainMetadataFeature() {
      super("domainMetadata", /* minWriterVersion = */ 7);
    }
  }

  public static final TableFeature ROW_TRACKING_W_FEATURE = new RowTrackingFeature();

  private static class RowTrackingFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    RowTrackingFeature() {
      super("rowTracking", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata);
    }

    @Override
    public Set<TableFeature> requiredFeatures() {
      return Collections.singleton(DOMAIN_METADATA_W_FEATURE);
    }
  }

  public static final TableFeature DELETION_VECTORS_RW_FEATURE = new DeletionVectorsTableFeature();

  /**
   * Kernel currently only support blind appends. So we don't need to do anything special for
   * writing into a table with deletion vectors enabled (i.e a table feature with DV enabled is both
   * readable and writable.
   */
  private static class DeletionVectorsTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    DeletionVectorsTableFeature() {
      super("deletionVectors", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.DELETION_VECTORS_CREATION_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature ICEBERG_COMPAT_V2_W_FEATURE = new IcebergCompatV2TableFeature();

  private static class IcebergCompatV2TableFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    IcebergCompatV2TableFeature() {
      super("icebergCompatV2", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    }

    public @Override Set<TableFeature> requiredFeatures() {
      return Collections.singleton(COLUMN_MAPPING_RW_FEATURE);
    }
  }

  public static final TableFeature TYPE_WIDENING_RW_FEATURE =
      new TypeWideningTableFeature("typeWidening");
  public static final TableFeature TYPE_WIDENING_PREVIEW_TABLE_FEATURE =
      new TypeWideningTableFeature("typeWidening-preview");

  private static class TypeWideningTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TypeWideningTableFeature(String featureName) {
      super(featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.TYPE_WIDENING_ENABLED.fromMetadata(metadata);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to support it.
    }
  }

  public static final TableFeature IN_COMMIT_TIMESTAMP_W_FEATURE =
      new InCommitTimestampTableFeature();

  private static class InCommitTimestampTableFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    InCommitTimestampTableFeature() {
      super("inCommitTimestamp", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature TIMESTAMP_NTZ_RW_FEATURE = new TimestampNtzTableFeature();

  private static class TimestampNtzTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TimestampNtzTableFeature() {
      super("timestampNtz", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasTypeColumn(metadata.getSchema(), TIMESTAMP_NTZ);
    }
  }

  public static final TableFeature CHECKPOINT_V2_RW_FEATURE = new CheckpointV2TableFeature();

  /**
   * In order to commit, there is no extra work required when v2 checkpoint is enabled. This affects
   * the checkpoint format only. When v2 is enabled, writing classic checkpoints is still allowed.
   */
  private static class CheckpointV2TableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    CheckpointV2TableFeature() {
      super("v2Checkpoint", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      // TODO: define an enum for checkpoint policy when we start supporting writing v2 checkpoints
      return "v2".equals(TableConfig.CHECKPOINT_POLICY.fromMetadata(metadata));
    }
  }

  public static final TableFeature VACUUM_PROTOCOL_CHECK_RW_FEATURE =
      new VacuumProtocolCheckTableFeature();

  private static class VacuumProtocolCheckTableFeature extends TableFeature.ReaderWriterFeature {
    VacuumProtocolCheckTableFeature() {
      super("vacuumProtocolCheck", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// END: Define the {@link TableFeature}s                                     ///
  /////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////
  /// Public static variables and methods                                       ///
  /////////////////////////////////////////////////////////////////////////////////
  /** Min reader version that supports reader features. */
  public static final int TABLE_FEATURES_MIN_READER_VERSION = 3;

  /** Min reader version that supports writer features. */
  public static final int TABLE_FEATURES_MIN_WRITER_VERSION = 7;

  public static final List<TableFeature> TABLE_FEATURES =
      Collections.unmodifiableList(
          Arrays.asList(
              APPEND_ONLY_W_FEATURE,
              CHECKPOINT_V2_RW_FEATURE,
              CHANGE_DATA_FEED_W_FEATURE,
              COLUMN_MAPPING_RW_FEATURE,
              CONSTRAINTS_W_FEATURE,
              DELETION_VECTORS_RW_FEATURE,
              GENERATED_COLUMNS_W_FEATURE,
              DOMAIN_METADATA_W_FEATURE,
              ICEBERG_COMPAT_V2_W_FEATURE,
              IDENTITY_COLUMNS_W_FEATURE,
              IN_COMMIT_TIMESTAMP_W_FEATURE,
              INVARIANTS_W_FEATURE,
              ROW_TRACKING_W_FEATURE,
              TIMESTAMP_NTZ_RW_FEATURE,
              TYPE_WIDENING_PREVIEW_TABLE_FEATURE,
              TYPE_WIDENING_RW_FEATURE,
              VACUUM_PROTOCOL_CHECK_RW_FEATURE,
              VARIANT_RW_FEATURE,
              VARIANT_RW_PREVIEW_FEATURE));

  public static final Map<String, TableFeature> TABLE_FEATURE_MAP =
      Collections.unmodifiableMap(
          new CaseInsensitiveMap<TableFeature>() {
            {
              for (TableFeature feature : TABLE_FEATURES) {
                put(feature.featureName(), feature);
              }
            }
          });

  /** Get the table feature by name. Case-insensitive lookup. If not found, throws error. */
  public static TableFeature getTableFeature(String featureName) {
    TableFeature tableFeature = TABLE_FEATURE_MAP.get(featureName);
    if (tableFeature == null) {
      throw DeltaErrors.unsupportedTableFeature(featureName);
    }
    return tableFeature;
  }

  /** Does reader version has support explicitly specifying reader feature set in protocol? */
  public static boolean supportsReaderFeatures(int minReaderVersion) {
    return minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION;
  }

  /** Does writer version has support explicitly specifying writer feature set in protocol? */
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
        protocol.getWriterFeatures().stream().map(TableFeatures::getTableFeature).collect(toSet());

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

  /** Utility method to check if the table with given protocol is readable by the Kernel. */
  public static void validateKernelCanReadTheTable(Protocol protocol, String tablePath) {
    Set<TableFeature> unsupportedFeatures =
        protocol.getImplicitlyAndExplicitlyEnabledFeatures().stream()
            .filter(f -> !f.hasKernelReadSupport())
            .collect(toSet());

    if (!unsupportedFeatures.isEmpty()) {
      throw unsupportedReaderFeatures(
          tablePath, unsupportedFeatures.stream().map(TableFeature::featureName).collect(toSet()));
    }
  }

  /**
   * Utility method to check if the table with given protocol and metadata is writable by the
   * Kernel.
   */
  public static void validateKernelCanWriteTheTable(
      Protocol protocol, Metadata metadata, String tablePath) {
    Set<TableFeature> unsupportedFeatures =
        protocol.getImplicitlyAndExplicitlyEnabledFeatures().stream()
            .filter(f -> !f.hasKernelWriteSupport(metadata))
            .filter(
                f -> {
                  if (f.isReaderWriterFeature()) {
                    return !f.hasKernelReadSupport();
                  } else {
                    return true;
                  }
                })
            .collect(toSet());

    if (!unsupportedFeatures.isEmpty()) {
      throw unsupportedWriterFeatures(
          tablePath, unsupportedFeatures.stream().map(TableFeature::featureName).collect(toSet()));
    }
  }

  public static boolean isRowTrackingEnabled(Protocol protocol) {
    return protocol.getImplicitlyAndExplicitlyEnabledFeatures().contains(ROW_TRACKING_W_FEATURE);
  }

  public static boolean isDomainMetadataEnabled(Protocol protocol) {
    return protocol.getImplicitlyAndExplicitlyEnabledFeatures().contains(DOMAIN_METADATA_W_FEATURE);
  }

  /**
   * Ensure all features listed in `currentFeatures` are also listed in `requiredFeatures`, or, if
   * one is not listed, it must be capable to auto-update a protocol.
   *
   * <p>Note: Caller must make sure `requiredFeatures` is obtained from a min protocol that
   * satisfies a table metadata.
   */
  private void assertMetadataTableFeaturesAutomaticallySupported(
      Set<TableFeature> currentFeatures, Set<TableFeature> requiredFeatures) {

    Set<TableFeature> newFeatures = new HashSet<>(requiredFeatures);
    newFeatures.removeAll(currentFeatures);

    List<TableFeature> nonAutoUpdateCapableFeatures =
        newFeatures.stream()
            .filter(f -> !(f instanceof FeatureAutoEnabledByMetadata))
            .collect(toList());

    if (!nonAutoUpdateCapableFeatures.isEmpty()) {
      // TODO: fix this error message to be more informative, may be a Kernel exception
      throw new UnsupportedOperationException(
          "The current protocol does not support auto upgrading following features"
              + nonAutoUpdateCapableFeatures);
    }
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

  private static boolean hasInvariants(StructType tableSchema) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ false, // invariants are not allowed in maps or
            // arrays
            // arrays
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.invariants"))
        .isEmpty();
  }

  private static boolean hasCheckConstraints(Metadata metadata) {
    return metadata.getConfiguration().entrySet().stream()
        .findAny()
        .map(entry -> entry.getKey().startsWith("delta.constraints."))
        .orElse(false);
  }

  /**
   * Check if the table schema has a column of type. Caution: works only for the primitive types.
   */
  private static boolean hasTypeColumn(StructType tableSchema, DataType type) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ true,
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getDataType().equals(type))
        .isEmpty();
  }

  private static boolean hasIdentityColumns(Metadata metadata) {
    return !SchemaUtils.filterRecursively(
            metadata.getSchema(),
            /* recurseIntoMapOrArrayElements = */ false, // don't expected identity columns in
            // nested columns
            /* stopOnFirstMatch */ true,
            /* filter */ field -> {
              FieldMetadata fieldMetadata = field.getMetadata();

              // Check if the metadata contains the required keys
              boolean hasStart = fieldMetadata.contains("delta.identity.start");
              boolean hasStep = fieldMetadata.contains("delta.identity.step");
              boolean hasInsert = fieldMetadata.contains("delta.identity.allowExplicitInsert");

              // Verify that all or none of the three fields are present
              if (!((hasStart == hasStep) && (hasStart == hasInsert))) {
                throw new KernelException(
                    String.format(
                        "Inconsistent IDENTITY metadata for column %s detected: %s, %s, %s",
                        field.getName(), hasStart, hasStep, hasInsert));
              }

              // Return true only if all three fields are present
              return hasStart && hasStep && hasInsert;
            })
        .isEmpty();
  }

  private static boolean hasGeneratedColumns(Metadata metadata) {
    return !SchemaUtils.filterRecursively(
            metadata.getSchema(),
            /* recurseIntoMapOrArrayElements = */ false, // don't expected generated columns in
            // nested columns
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.generationExpression"))
        .isEmpty();
  }
}
