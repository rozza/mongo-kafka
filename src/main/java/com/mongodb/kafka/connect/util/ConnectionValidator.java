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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;


public final class ConnectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionValidator.class);

    public static Optional<MongoClient> validateCanConnect(final Config config, final String connectionStringConfigName) {
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

    public static void validateUserHasActions(final MongoClient mongoClient, final MongoCredential credential, final List<String> actions,
                                              final String databaseName, final String collectionName, final String configName,
                                              final Config config) {

        if (credential == null) {
            return;
        }

        try {
            Document usersInfo = mongoClient.getDatabase(credential.getSource())
                    .runCommand(Document.parse(format("{usersInfo: '%s', showPrivileges: 1}", credential.getUserName())));

            List<String> unsupportedUserActions = new ArrayList<>(actions);
            for (final Document userInfo : usersInfo.getList("users", Document.class)) {
                unsupportedUserActions = removeUserActions(userInfo, databaseName, collectionName, actions);
                if (!unsupportedUserActions.isEmpty() && userInfo.getList("inheritedPrivileges", Document.class).isEmpty()) {
                    for (final Document inheritedRole : userInfo.getList("inheritedRoles", Document.class)) {
                        String roleDB = inheritedRole.getString("db");
                        String roleName = inheritedRole.getString("role");
                        Document rolesInfo = mongoClient.getDatabase(roleDB)
                                .runCommand(Document.parse(format("{rolesInfo: '%s', showPrivileges: 1, showBuiltinRoles: 1}", roleName)));
                        for (final Document roleInfo : rolesInfo.getList("roles", Document.class)) {
                            unsupportedUserActions = removeUserActions(roleInfo, databaseName, collectionName, unsupportedUserActions);
                        }

                        if (unsupportedUserActions.isEmpty()) {
                            return;
                        }
                    }
                }
                if (unsupportedUserActions.isEmpty()) {
                    return;
                }
            }

            String missingPermissions = String.join(", ", unsupportedUserActions);
            getConfigByName(config, configName).ifPresent(c ->
                    c.addErrorMessage(format("Invalid user permissions. Missing the following action permissions: %s", missingPermissions))
            );
        } catch (MongoSecurityException e) {
            getConfigByName(config, configName).ifPresent(c ->
                    c.addErrorMessage(format("Invalid user permissions authentication failed."))
            );
        } catch (Exception e) {
            LOGGER.warn("Permission validation failed due to: {}", e.getMessage(), e);
        }
    }

    private static List<String> removeUserActions(final Document rolesInfo, final String databaseName, final String collectionName,
                                          final List<String> userActions) {
        List<Document> privileges = rolesInfo.getList("inheritedPrivileges", Document.class);
        if (privileges.isEmpty() || userActions.isEmpty()) {
            return userActions;
        }

        List<String> unsupportedUserActions = new ArrayList<>(userActions);
        for (final Document privilege : privileges) {
            Document resource = privilege.get("resource", Document.class);
            String database = resource.getString("db");
            String collection = resource.getString("collection");

            if (database.isEmpty() || (database.equals(databaseName) && collection.isEmpty())
                    || (database.equals(databaseName) && collection.equals(collectionName))) {
                unsupportedUserActions.removeAll(privilege.getList("actions", String.class));
            }

            if (unsupportedUserActions.isEmpty()) {
                break;
            }
        }

        return unsupportedUserActions;
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