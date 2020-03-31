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
package com.mongodb.kafka.connect.util;

import static java.lang.String.format;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

public final class ConnectionValidator {

    public static Optional<MongoClient> validateConnection(final Config config, final String connectionStringConfigName) {
        Optional<ConfigValue> optionalConnectionString = getConfigByName(config, connectionStringConfigName);
        if (optionalConnectionString.isPresent() && optionalConnectionString.get().errorMessages().isEmpty()) {
            ConfigValue configValue = optionalConnectionString.get();

            AtomicBoolean connected = new AtomicBoolean();
            CountDownLatch latch = new CountDownLatch(1);
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString((String) configValue.value()))
                    .applyToClusterSettings(b -> b.addClusterListener(new ClusterListener() {
                        @Override
                        public void clusterOpening(final ClusterOpeningEvent event) {
                        }

                        @Override
                        public void clusterClosed(final ClusterClosedEvent event) {
                        }

                        @Override
                        public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                            if (!connected.get() && event.getNewDescription().hasReadableServer(ReadPreference.primaryPreferred())) {
                                connected.set(true);
                                latch.countDown();

                            }
                        }
                    }))
                    .build();

            long latchTimeout = mongoClientSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) + 500;
            MongoClient mongoClient = MongoClients.create(mongoClientSettings);

            try {
                if (!latch.await(latchTimeout, TimeUnit.MILLISECONDS)) {
                    throw new Exception("Didn't connect");
                }
            } catch (Exception e) {
                configValue.addErrorMessage("Invalid connection string, unable to reach the server.");
                mongoClient.close();
            }
            if (configValue.errorMessages().isEmpty()) {
                return Optional.of(mongoClient);
            }
        }
        return Optional.empty();
    }

    public static void validateHasChangeStreamPermissions(final MongoClient mongoClient, final MongoSourceConfig sourceConfig,
                                                          final Config config) {

        String database = sourceConfig.getString(MongoSourceConfig.DATABASE_CONFIG);
        String collection = sourceConfig.getString(MongoSourceConfig.COLLECTION_CONFIG);

        Optional<List<Document>> pipeline = sourceConfig.getPipeline();
        ChangeStreamIterable<Document> changeStream;
        if (database.isEmpty()) {
            changeStream = pipeline.map(mongoClient::watch).orElse(mongoClient.watch());
        } else if (collection.isEmpty()) {
            MongoDatabase db = mongoClient.getDatabase(database);
            changeStream = pipeline.map(db::watch).orElse(db.watch());
        } else {
            MongoCollection<Document> coll = mongoClient.getDatabase(database).getCollection(collection);
            changeStream = pipeline.map(coll::watch).orElse(coll.watch());
        }

        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
            cursor.tryNext();
        } catch (Exception e) {
            getConfigByName(config, MongoSourceConfig.CONNECTION_URI_CONFIG).ifPresent(c ->
                    c.addErrorMessage(format("Unable to create a change stream cursor: %s ", e.getMessage()))
            );
        }
    }

    public static void validateHasReadWritePermissions(final MongoClient mongoClient, final MongoSinkConfig sinkConfig,
                                                       final Config config) {
        for (final String topic : sinkConfig.getTopics()) {
            MongoSinkTopicConfig mongoSinkTopicConfig = sinkConfig.getMongoSinkTopicConfig(topic);
            MongoNamespace namespace = mongoSinkTopicConfig.getNamespace();

            boolean hasReadWritePermissions = false;
            try {
                hasReadWritePermissions = !mongoClient.getDatabase(namespace.getDatabaseName())
                        .runCommand(BsonDocument.parse(format("{rolesInfo: {role: 'readWrite', db: '%s' }}", namespace.getDatabaseName())),
                                BsonDocument.class)
                        .getArray("roles", new BsonArray())
                        .isEmpty();
            } catch (MongoCommandException e) {
                if (e.getErrorCode() == 13) {
                    // Not Authorized to run rolesInfo command.
                    hasReadWritePermissions = true; // We can't check as the user doesn't have the perms
                } else if (e.getErrorCode() == 8000 && e.getErrorMessage().contains("user is not allowed to do action")) {
                    // Not Authorized on Atlas to run the command
                    hasReadWritePermissions = true; // We can't check as the user doesn't have the perms
                }
            } catch (Exception e) {
                // Ignore any security or other exceptions
            }
            if (!hasReadWritePermissions){
                getConfigByName(config, MongoSinkConfig.CONNECTION_URI_CONFIG).ifPresent(c ->
                    c.addErrorMessage(format("Invalid user permissions cannot write to: %s", namespace.getFullName()))
                );
                break;
            }
        }
    }

    private static Optional<ConfigValue> getConfigByName(final Config config, final String name) {
        for (final ConfigValue configValue : config.configValues()) {
            if (configValue.name().equals(name)) {
                return Optional.of(configValue);
            }
        }
        return Optional.empty();
    }

    private ConnectionValidator() {
    }
}
