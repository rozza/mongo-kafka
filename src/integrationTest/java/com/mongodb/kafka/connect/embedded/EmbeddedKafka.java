/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2018 Confluent Inc.
 */
package com.mongodb.kafka.connect.embedded;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, 1
 * Confluent Schema Registry instance, and 1 Confluent Connect instance.
 */
public class EmbeddedKafka implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafka.class);
  private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
  private static final String COMPATIBILITY_LEVEL = "BACKWARD";

  private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "10000";
  private static final String KAFKASTORE_DEBUG = "true";
  private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

  private static final String SINK_CONNECTOR_NAME = "MongoSinkConnector";
  private static final String SOURCE_CONNECTOR_NAME = "MongoSourceConnector";
  private final Properties brokerConfig;
  private EmbeddedKafkaCluster cluster;
  private ConnectStandalone connect;
  private RestApp schemaRegistry;
  private boolean running;
  private boolean addedSink;
  private boolean addedSource;
  private List<String> topics = new ArrayList<>();

  /** Creates and starts the cluster. */
  public EmbeddedKafka() {
    this(new Properties());
  }

  /**
   * Creates and starts the cluster.
   *
   * @param brokerConfig Additional broker configuration settings.
   */
  public EmbeddedKafka(final Properties brokerConfig) {
    Properties brokerProps = new Properties();
    brokerProps.putAll(brokerConfig);

    // If log.dir is not set.
    if (brokerProps.getProperty("log.dir") == null) {
      // Create temp path to store logs and set property.
      brokerProps.setProperty("log.dir", createTempDirectory("Logs"));
    }

    // Ensure that we're advertising correct hostname appropriately
    brokerProps.setProperty("host.name", brokerProps.getProperty("host.name", "localhost"));

    brokerProps.setProperty(
        "auto.create.topics.enable", brokerProps.getProperty("auto.create.topics.enable", "true"));
    brokerProps.setProperty(
        "zookeeper.session.timeout.ms",
        brokerProps.getProperty("zookeeper.session.timeout.ms", "30000"));
    brokerProps.setProperty("broker.id", brokerProps.getProperty("broker.id", "1"));
    brokerProps.setProperty(
        "auto.offset.reset", brokerProps.getProperty("auto.offset.reset", "latest"));

    // Lower active threads.
    brokerProps.setProperty("num.io.threads", brokerProps.getProperty("num.io.threads", "2"));
    brokerProps.setProperty(
        "num.network.threads", brokerProps.getProperty("num.network.threads", "2"));
    brokerProps.setProperty(
        "log.flush.interval.messages",
        brokerProps.getProperty("log.flush.interval.messages", "100"));

    // Define replication factor for internal topics to 1
    brokerProps.setProperty(
        "offsets.topic.replication.factor",
        brokerProps.getProperty("offsets.topic.replication.factor", "1"));
    brokerProps.setProperty(
        "offset.storage.replication.factor",
        brokerProps.getProperty("offset.storage.replication.factor", "1"));
    brokerProps.setProperty(
        "transaction.state.log.replication.factor",
        brokerProps.getProperty("transaction.state.log.replication.factor", "1"));
    brokerProps.setProperty(
        "transaction.state.log.min.isr",
        brokerProps.getProperty("transaction.state.log.min.isr", "1"));
    brokerProps.setProperty(
        "transaction.state.log.num.partitions",
        brokerProps.getProperty("transaction.state.log.num.partitions", "4"));
    brokerProps.setProperty(
        "config.storage.replication.factor",
        brokerProps.getProperty("config.storage.replication.factor", "1"));
    brokerProps.setProperty(
        "status.storage.replication.factor",
        brokerProps.getProperty("status.storage.replication.factor", "1"));
    brokerProps.setProperty(
        "default.replication.factor", brokerProps.getProperty("default.replication.factor", "1"));

    this.brokerConfig = brokerProps;
  }

  private static String createTempDirectory(final String suffix) {
    try {
      Path logDir = Files.createTempFile("kafkaConnect", suffix);
      logDir.toFile().deleteOnExit();
      return logDir.toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Creates and starts the cluster. */
  public void start() throws Exception {
    LOGGER.debug("Initiating embedded Kafka cluster startup");
    cluster = new EmbeddedKafkaCluster(1, brokerConfig);
    cluster.start();

    final Properties schemaRegistryProps = new Properties();
    schemaRegistryProps.put(
        SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
    schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
    schemaRegistryProps.put(
        SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);

    schemaRegistry =
        new RestApp(
            bootstrapServers(),
            KAFKA_SCHEMAS_TOPIC,
            COMPATIBILITY_LEVEL,
            true,
            schemaRegistryProps);
    schemaRegistry.start();

    LOGGER.debug("Starting a Connect standalone instance...");
    connect = new ConnectStandalone(connectWorkerConfig());
    connect.start();
    LOGGER.debug("Connect standalone instance is running at {}", connect.getConnectionString());
    running = true;
  }

  public void addSinkConnector(final Properties properties) {
    properties.put("name", SINK_CONNECTOR_NAME);
    LOGGER.info("Adding connector: {}", properties);
    connect.addConnector(SINK_CONNECTOR_NAME, properties);
    addedSink = true;
  }

  public void restartSinkConnector() {
    if (addedSource) {
      connect.restartConnector(SOURCE_CONNECTOR_NAME);
    }
  }

  public void deleteSinkConnector() {
    if (addedSink) {
      connect.deleteConnector(SINK_CONNECTOR_NAME);
      addedSink = false;
    }
  }

  public void addSourceConnector(final Properties properties) {
    properties.put("name", SOURCE_CONNECTOR_NAME);
    LOGGER.info("Adding connector: {}", properties);
    connect.addConnector(SOURCE_CONNECTOR_NAME, properties);
    addedSource = true;
  }

  public void restartSourceConnector() {
    if (addedSource) {
      connect.restartConnector(SOURCE_CONNECTOR_NAME);
    }
  }

  public void deleteSourceConnector() {
    if (addedSource) {
      connect.deleteConnector(SOURCE_CONNECTOR_NAME);
      addedSource = false;
    }
  }

  public void resetOffsets() {
    connect.resetOffsets();
  }

  private Properties connectWorkerConfig() {
    Properties workerProps = new Properties();
    workerProps.put(StandaloneConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    workerProps.put(
        StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG,
        "org.apache.kafka.connect.storage.StringConverter");
    workerProps.put("key.converter.schemas.enable", "false");
    workerProps.put(
        StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG,
        "org.apache.kafka.connect.storage.StringConverter");
    workerProps.put("value.converter.schemas.enable", "false");
    workerProps.put(
        StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, createTempDirectory("Offsets"));

    return workerProps;
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    start();
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    resetOffsets();
    deleteSinkConnector();
    deleteSourceConnector();
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    stop();
  }

  /** Stops the cluster. */
  public void stop() {
    if (running) {
      LOGGER.info("Stopping Confluent");
      try {
        if (connect != null) {
          connect.stop();
        }
        try {
          if (schemaRegistry != null) {
            schemaRegistry.stop();
          }
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
        if (cluster != null) {
          cluster.stop();
        }
      } finally {
        running = false;
      }
      LOGGER.info("Confluent Stopped");
    }
  }

  /**
   * This cluster's `bootstrap.servers` value. Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka Streams applications, Kafka producers, and Kafka consumers
   * (new consumer API) how to connect to this cluster.
   */
  public String bootstrapServers() {
    return cluster.bootstrapServers();
  }

  /** The "schema.registry.url" setting of the schema registry instance. */
  public String schemaRegistryUrl() {
    return schemaRegistry.restConnect;
  }

  /**
   * Creates a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(final String topic) throws InterruptedException {
    createTopic(topic, 1, 1, emptyMap());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic The name of the topic.
   * @param partitions The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(final String topic, final int partitions, final int replication)
      throws InterruptedException {
    createTopic(topic, partitions, replication, emptyMap());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic The name of the topic.
   * @param partitions The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(
      final String topic,
      final int partitions,
      final int replication,
      final Map<String, String> topicConfig)
      throws InterruptedException {
    topics.add(topic);
    cluster.createTopic(topic, partitions, replication, topicConfig);
  }
}
