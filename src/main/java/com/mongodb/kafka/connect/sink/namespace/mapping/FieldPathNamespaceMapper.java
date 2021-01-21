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

package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.NAMESPACE_MAPPER_CONFIG;
import static java.lang.String.format;

import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class FieldPathNamespaceMapper implements NamespaceMapper {

  private String defaultDatabaseName;
  private String defaultCollectionName;
  private String keyDatabaseFieldPath;
  private String valueDatabaseFieldPath;
  private String keyCollectionFieldPath;
  private String valueCollectionFieldPath;

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    this.defaultDatabaseName = config.getString(DATABASE_CONFIG);
    this.defaultCollectionName = config.getString(COLLECTION_CONFIG);
    if (this.defaultCollectionName.isEmpty()) {
      this.defaultCollectionName = config.getTopic();
    }

    this.keyDatabaseFieldPath = config.getString(FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG);
    this.valueDatabaseFieldPath = config.getString(FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG);
    this.keyCollectionFieldPath = config.getString(FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG);
    this.valueCollectionFieldPath =
        config.getString(FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG);

    if (keyDatabaseFieldPath.isEmpty()
        && valueDatabaseFieldPath.isEmpty()
        && keyCollectionFieldPath.isEmpty()
        && valueCollectionFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          NAMESPACE_MAPPER_CONFIG,
          config.getString(NAMESPACE_MAPPER_CONFIG),
          "Missing configuration for the FieldBasedNamespaceMapper. "
              + "Please configure the database and / or collection field path.");
    }

    if (!keyDatabaseFieldPath.isEmpty() && !valueDatabaseFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
          keyDatabaseFieldPath,
          format(
              "Cannot set both: '%s' and '%s'",
              FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
              FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG));
    } else if (!keyCollectionFieldPath.isEmpty() && !valueCollectionFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
          keyCollectionFieldPath,
          format(
              "Cannot set both: '%s' and '%s'",
              FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
              FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG));
    }
  }

  @Override
  public MongoNamespace getNamespace(final SinkRecord sinkRecord, final SinkDocument sinkDocument) {
    String databaseName = defaultDatabaseName;
    String collectionName = defaultCollectionName;

    if (!keyDatabaseFieldPath.isEmpty()) {
      databaseName = getFromPath(sinkRecord, sinkDocument, keyDatabaseFieldPath, true);
    } else if (!valueDatabaseFieldPath.isEmpty()) {
      databaseName = getFromPath(sinkRecord, sinkDocument, valueDatabaseFieldPath, false);
    }

    if (!keyCollectionFieldPath.isEmpty()) {
      collectionName = getFromPath(sinkRecord, sinkDocument, keyCollectionFieldPath, true);
    } else if (!valueCollectionFieldPath.isEmpty()) {
      collectionName = getFromPath(sinkRecord, sinkDocument, valueCollectionFieldPath, false);
    }

    return new MongoNamespace(databaseName, collectionName);
  }

  private String getFromPath(
      final SinkRecord sinkRecord,
      final SinkDocument sinkDocument,
      final String path,
      final boolean isKey) {
    Optional<BsonDocument> optionalData =
        isKey ? sinkDocument.getKeyDoc() : sinkDocument.getValueDoc();
    BsonDocument data =
        optionalData.orElseThrow(
            () ->
                new DataException(
                    format(
                        "Missing or invalid %s document: %s",
                        isKey ? "key" : "value", sinkRecord)));

    try {
      return data.getString(path).getValue();
    } catch (BsonInvalidOperationException e) {
      throw new DataException(
          format(
              "Missing or invalid %s document: %s. %s",
              isKey ? "key" : "value", sinkRecord, e.getMessage()));
    }
  }
}
