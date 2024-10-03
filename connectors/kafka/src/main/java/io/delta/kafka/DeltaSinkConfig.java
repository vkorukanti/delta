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

import static io.delta.kafka.DeltaKafkaMeta.DELTA_KAFKA_CONNECT_VERSION;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.util.Preconditions;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.shaded.com.google.common.base.Splitter;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

public class DeltaSinkConfig extends AbstractConfig {
  private static final String KAFKA_PROP_PREFIX = "delta.kafka.";

  private static final String TABLES_PROP = "delta.tables";
  private static final String CONTROL_TOPIC_PROP = "delta.control.topic";
  private static final String CONNECT_GROUP_ID_PROP = "delta.connect.group-id";
  private static final String NAME_PROP = "name";
  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "delta.coordinator.transactional.suffix";
  private static final String TABLES_SCHEMA_FORCE_OPTIONAL_PROP =
      "delta.tables.schema-force-optional";
  private static final String COMMIT_INTERVAL_MS_PROP = "delta.control.commit.interval-ms";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "delta.control.commit.timeout-ms";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;

  public static final String DEFAULT_CONTROL_GROUP_PREFIX = "cg-control-";
  private static final String DEFAULT_CONTROL_TOPIC = "control-delta";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  private static final String BOOTSTRAP_SERVERS_PROP = "bootstrap.servers";

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        TABLES_PROP,
        ConfigDef.Type.LIST,
        null,
        ConfigDef.Importance.HIGH,
        "Comma-delimited list of destination tables");
    configDef.define(
        CONTROL_TOPIC_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CONTROL_TOPIC,
        ConfigDef.Importance.MEDIUM,
        "Name of the control topic");
    configDef.define(
        CONNECT_GROUP_ID_PROP,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.LOW,
        "Name of the Connect consumer group, should not be set under normal conditions");
    configDef.define(
        COMMIT_INTERVAL_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_INTERVAL_MS_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        "Coordinator interval for performing Iceberg table commits, in millis");
    configDef.define(
        COMMIT_TIMEOUT_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_TIMEOUT_MS_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        "Coordinator time to wait for worker responses before committing, in millis");

    return configDef;
  }

  public static String version() {
    return DELTA_KAFKA_CONNECT_VERSION;
  }

  private final Map<String, String> originalProps;
  private final Map<String, String> kafkaProps;
  private final JsonConverter jsonConverter;

  public DeltaSinkConfig(Map<String, String> originalProps) {
    super(CONFIG_DEF, originalProps);
    this.originalProps = originalProps;

    this.kafkaProps = Maps.newHashMap(loadWorkerProps());
    kafkaProps.putAll(propertiesWithPrefix(originalProps, KAFKA_PROP_PREFIX));

    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));

    validate();
  }

  public String controlTopic() {
    return getString(CONTROL_TOPIC_PROP);
  }

  public String connectGroupId() {
    String result = getString(CONNECT_GROUP_ID_PROP);
    if (result != null) {
      return result;
    }

    String connectorName = connectorName();
    requireNonNull(connectorName, "Connector name cannot be null");
    return "connect-" + connectorName;
  }

  public String connectorName() {
    return originalProps.get(NAME_PROP);
  }

  public String transactionalSuffix() {
    // this is for internal use and is not part of the config definition...
    return originalProps.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public boolean schemaForceOptional() {
    return getBoolean(TABLES_SCHEMA_FORCE_OPTIONAL_PROP);
  }

  public int commitIntervalMs() {
    return getInt(COMMIT_INTERVAL_MS_PROP);
  }

  public int commitTimeoutMs() {
    return getInt(COMMIT_TIMEOUT_MS_PROP);
  }

  private void validate() {
    // TODO: implement
  }

  public Map<String, String> kafkaProps() {
    return kafkaProps;
  }

  /**
   * This method attempts to load the Kafka Connect worker properties, which are not exposed to
   * connectors. It does this by parsing the Java command used to launch the worker, extracting the
   * name of the properties file, and then loading the file. <br>
   * The sink uses these properties, if available, when initializing its internal Kafka clients. By
   * doing this, Kafka-related properties only need to be set in the worker properties and do not
   * need to be duplicated in the sink config. <br>
   * If the worker properties cannot be loaded, then Kafka-related properties must be set via the
   * `iceberg.kafka.*` sink configs.
   *
   * @return The Kafka Connect worker properties
   */
  private Map<String, String> loadWorkerProps() {
    String javaCmd = System.getProperty("sun.java.command");
    if (javaCmd != null && !javaCmd.isEmpty()) {
      List<String> args = Splitter.on(' ').splitToList(javaCmd);
      if (args.size() > 1
          && (args.get(0).endsWith(".ConnectDistributed")
              || args.get(0).endsWith(".ConnectStandalone"))) {
        Properties result = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args.get(1)))) {
          result.load(in);
          // sanity check that this is the config we want
          if (result.containsKey(BOOTSTRAP_SERVERS_PROP)) {
            return Maps.fromProperties(result);
          }
        } catch (Exception e) {
          // NO-OP
        }
      }
    }
    return ImmutableMap.of();
  }

  /**
   * Returns subset of provided map with keys matching the provided prefix. Matching is
   * case-sensitive and the matching prefix is removed from the keys in returned map.
   *
   * @param properties input map
   * @param prefix prefix to choose keys from input map
   * @return subset of input map with keys starting with provided prefix and prefix trimmed out
   */
  public static Map<String, String> propertiesWithPrefix(
      Map<String, String> properties, String prefix) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    Preconditions.checkArgument(prefix != null, "Invalid prefix: null");

    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(e -> e.getKey().replaceFirst(prefix, ""), Map.Entry::getValue));
  }
}
