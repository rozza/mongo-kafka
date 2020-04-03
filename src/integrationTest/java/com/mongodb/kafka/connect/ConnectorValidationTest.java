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
 */

package com.mongodb.kafka.connect;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

public final class ConnectorValidationTest {

    private static final String DEFAULT_URI = "mongodb://localhost:27017";
    private static final String URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "MongoKafkaTest";

    @Test
    @DisplayName("Ensure sink configuration validation works")
    void testSinkConfigValidation() {
        Map<String, String> properties = new HashMap<>();
        properties.put("topics", "test");
        properties.put(MongoSinkConfig.CONNECTION_URI_CONFIG, getConnectionString().toString());
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSinkConnector().validate(properties);
        config.configValues().forEach(configValue -> assertTrue(configValue.errorMessages().isEmpty()));
    }

    @Test
    @DisplayName("Ensure sink configuration validation handles invalid connections")
    void testSinkConfigValidationInvalidConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("topics", "test");
        properties.put(MongoSinkConfig.CONNECTION_URI_CONFIG, "mongodb://192.0.2.0:27017/?connectTimeoutMS=1000");
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSinkConnector().validate(properties);
        assertFalse(getConfigValue(config, MongoSinkConfig.CONNECTION_URI_CONFIG).errorMessages().isEmpty());
    }

    @Test
    @DisplayName("Ensure sink configuration validation handles invalid user")
    void testSinkConfigValidationInvalidUser() {
        Map<String, String> properties = new HashMap<>();
        properties.put("topics", "test");
        properties.put(MongoSinkConfig.CONNECTION_URI_CONFIG, format("mongodb://fakeUser:fakePass@%s/",
                String.join(",", getConnectionString().getHosts())));
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSinkConnector().validate(properties);
        assertFalse(getConfigValue(config, MongoSinkConfig.CONNECTION_URI_CONFIG).errorMessages().isEmpty());
    }

    @Test
    @DisplayName("Ensure source configuration validation works")
    void testSourceConfigValidation() {
        assumeTrue(isReplicaSetOrSharded());
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoSourceConfig.CONNECTION_URI_CONFIG, getConnectionString().toString());
        properties.put(MongoSourceConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSourceConnector().validate(properties);
        config.configValues().forEach(configValue -> assertTrue(configValue.errorMessages().isEmpty()));
    }

    @Test
    @DisplayName("Ensure source configuration validation handles invalid connections")
    void testSourceConfigValidationInvalidConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoSourceConfig.CONNECTION_URI_CONFIG, "mongodb://192.0.2.0:27017/?connectTimeoutMS=1000");
        properties.put(MongoSourceConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSourceConnector().validate(properties);
        assertFalse(getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages().isEmpty());
    }

    @Test
    @DisplayName("Ensure source configuration validation handles invalid user")
    void testSourceConfigValidationInvalidUser() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoSourceConfig.CONNECTION_URI_CONFIG, format("mongodb://fakeUser:fakePass@%s/",
                String.join(",", getConnectionString().getHosts())));
        properties.put(MongoSourceConfig.DATABASE_CONFIG, getDatabaseName());

        Config config = new MongoSourceConnector().validate(properties);
        assertFalse(getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages().isEmpty());
    }


    boolean isReplicaSetOrSharded() {
        try (MongoClient mongoClient = MongoClients.create(getConnectionString())) {
            Document isMaster = mongoClient.getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
            return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
        } catch (Exception e) {
            return false;
        }
    }

    ConfigValue getConfigValue(final Config config, final String configName) {
        return config.configValues().stream().filter(cv -> cv.name().equals(configName)).collect(Collectors.toList()).get(0);
    }

    String getDatabaseName() {
        String databaseName = getConnectionString().getDatabase();
        return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
    }

    ConnectionString getConnectionString() {
        String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
        String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
        return new ConnectionString(mongoURIString);
    }
}
