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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler;
import org.apache.kafka.connect.storage.StringConverter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.concat;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createInserts;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createUpdates;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.*;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ChangeStreamRoundTripTest extends MongoKafkaTestCase {

    @BeforeEach
    void setUp() {
        assumeTrue(isReplicaSetOrSharded());
    }

    @AfterEach
    void tearDown() {
        getMongoClient().listDatabaseNames().into(new ArrayList<>()).forEach(i -> {
            if (i.startsWith(getDatabaseName())) {
                getMongoClient().getDatabase(i).drop();
            }
        });
    }

    @Test
    @DisplayName("Ensure collection CRUD operations are replicated")
    void testCollectionReplicationCrud() throws InterruptedException {
        MongoDatabase database = getDatabaseWithPostfix();
        MongoCollection<Document> original = database.getCollection("original");
        MongoCollection<Document> replicated = database.getCollection("replicated");

        Properties sourceProperties = new Properties();
        sourceProperties.put(DATABASE_CONFIG, original.getNamespace().getDatabaseName());
        sourceProperties.put(COLLECTION_CONFIG, original.getNamespace().getCollectionName());
        sourceProperties.put(TOPIC_PREFIX_CONFIG, "copy");
        sourceProperties.put(COPY_EXISTING_CONFIG, "true");
        addSourceConnector(sourceProperties);

        Properties sinkProperties = new Properties();
        sinkProperties.put("topics", format("copy.%s.%s", original.getNamespace().getDatabaseName(),
                original.getNamespace().getCollectionName()));
        sinkProperties.put(DATABASE_CONFIG, replicated.getNamespace().getDatabaseName());
        sinkProperties.put(COLLECTION_CONFIG, "replicated");
        sinkProperties.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, ChangeStreamHandler.class.getName());
        sinkProperties.put("key.converter", StringConverter.class.getName());
        sinkProperties.put("value.converter", StringConverter.class.getName());

        addSinkConnector(sinkProperties);

        Thread.sleep(5000);

        insertMany(rangeClosed(1, 10), original);
        original.updateMany(new Document(), Updates.set("test", 1));

        assertCollection(original, replicated);

        original.replaceOne(Filters.eq("_id", 1), new Document());
        original.replaceOne(Filters.eq("_id", 3), new Document());
        original.replaceOne(Filters.eq("_id", 5), new Document());
        assertCollection(original, replicated);

        original.deleteMany(Filters.mod("_id", 2, 0));
        assertCollection(original, replicated);
    }

    @Test
    @DisplayName("Ensure collections can be created, renamed, deleted and recreated")
    void testCollectionRenameAndDeleteAndRecreate() throws InterruptedException {
        MongoDatabase database = getDatabaseWithPostfix();
        MongoCollection<Document> original = database.getCollection("original");
        MongoCollection<Document> originalRenamed = database.getCollection("originalRenamed");
        MongoCollection<Document> replicated = database.getCollection("replicated");
        MongoCollection<Document> replicatedRenamed = database.getCollection("replicatedRenamed");

        Properties sourceProperties = new Properties();
        sourceProperties.put(DATABASE_CONFIG, original.getNamespace().getDatabaseName());
        sourceProperties.put(TOPIC_PREFIX_CONFIG, "copy");
        sourceProperties.put(COPY_EXISTING_CONFIG, "true");
        addSourceConnector(sourceProperties);

        Properties sinkProperties = new Properties();
        sinkProperties.put(TOPICS_CONFIG, format("copy.%s.%s", original.getNamespace().getDatabaseName(),
                original.getNamespace().getCollectionName()));
        sinkProperties.put(DATABASE_CONFIG, replicated.getNamespace().getDatabaseName());
        sinkProperties.put(COLLECTION_CONFIG, replicated.getNamespace().getCollectionName());
        sinkProperties.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, ChangeStreamHandler.class.getName());
        sinkProperties.put("key.converter", StringConverter.class.getName());
        sinkProperties.put("value.converter", StringConverter.class.getName());
        addSinkConnector(sinkProperties);

        sinkProperties = new Properties();
        sinkProperties.put(TOPICS_CONFIG, format("copy.%s.%s", originalRenamed.getNamespace().getDatabaseName(),
                originalRenamed.getNamespace().getCollectionName()));
        sinkProperties.put(DATABASE_CONFIG, replicatedRenamed.getNamespace().getDatabaseName());
        sinkProperties.put(COLLECTION_CONFIG, replicatedRenamed.getNamespace().getCollectionName());
        sinkProperties.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, ChangeStreamHandler.class.getName());
        sinkProperties.put("key.converter", StringConverter.class.getName());
        sinkProperties.put("value.converter", StringConverter.class.getName());
        addSinkConnector(sinkProperties);

        Thread.sleep(5000);

        insertMany(rangeClosed(1, 10), original);
        assertCollection(original, replicated);
        assertEquals(0, originalRenamed.countDocuments());
        assertEquals(0, replicatedRenamed.countDocuments());

        original.renameCollection(originalRenamed.getNamespace());
        assertCollection(originalRenamed, replicatedRenamed);
        assertEquals(0, original.countDocuments());
        assertEquals(0, replicated.countDocuments());
    }

    private MongoDatabase getDatabaseWithPostfix() {
        return getMongoClient().getDatabase(format("%s%s", getDatabaseName(), POSTFIX.incrementAndGet()));
    }

    private List<Document> insertMany(final IntStream stream, final MongoCollection<?>... collections) {
        List<Document> docs = stream.mapToObj(i -> Document.parse(format("{_id: %s}", i))).collect(toList());
        for (MongoCollection<?> c : collections) {
            LOGGER.debug("Inserting into {} ", c.getNamespace().getFullName());
            c.withDocumentClass(Document.class).insertMany(docs);
        }
        return docs;
    }

}
