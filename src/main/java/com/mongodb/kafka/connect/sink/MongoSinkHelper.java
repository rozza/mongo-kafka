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
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.sink;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_NUM_RETRIES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.RETRIES_DEFER_TIMEOUT_CONFIG;

public class MongoSinkHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkHelper.class);
    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();
    private static final Map<String, AtomicInteger> REMAINING_RETRIES_TOPIC_MAP = new ConcurrentHashMap<>();

    public static AtomicInteger getRemainingRetries(final MongoSinkTopicConfig config) {
        return REMAINING_RETRIES_TOPIC_MAP.getOrDefault(config.getTopic(), new AtomicInteger(config.getInt(MAX_NUM_RETRIES_CONFIG)));
    }

    public static void resetRetriesForTopic(final MongoSinkTopicConfig config) {
        REMAINING_RETRIES_TOPIC_MAP.remove(config.getTopic());
    }

    public static void checkRetriableException(final SinkTaskContext context, final MongoSinkTopicConfig config, final MongoException e) {
        if (REMAINING_RETRIES_TOPIC_MAP.get(config.getTopic()).decrementAndGet() <= 0) {
            throw new DataException("Failed to write mongodb documents despite retrying", e);
        }
        Integer deferRetryMs = config.getInt(RETRIES_DEFER_TIMEOUT_CONFIG);
        LOGGER.debug("Deferring retry operation for {}ms", deferRetryMs);
        context.timeout(deferRetryMs);
        throw new RetriableException(e.getMessage(), e);
    }

    public static void processWriteModels(final SinkTaskContext context, final MongoSinkTopicConfig config,
                                          final MongoClient mongoClient, final List<? extends WriteModel<BsonDocument>> writeModels) {
        MongoNamespace namespace = config.getNamespace();
        try {
            if (!writeModels.isEmpty()) {
                LOGGER.debug("Bulk writing {} document(s) into collection [{}]", writeModels.size(), namespace.getFullName());
                BulkWriteResult result = mongoClient
                        .getDatabase(namespace.getDatabaseName())
                        .getCollection(namespace.getCollectionName(), BsonDocument.class)
                        .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
                LOGGER.debug("Mongodb bulk write result: {}", result);
            }
        } catch (MongoBulkWriteException e) {
            LOGGER.error("Mongodb bulk write (partially) failed", e);
            LOGGER.error(e.getWriteResult().toString());
            LOGGER.error(e.getWriteErrors().toString());
            LOGGER.error(e.getWriteConcernError().toString());
            checkRetriableException(context, config, e);
        } catch (MongoException e) {
            LOGGER.error("Error on mongodb operation", e);
            LOGGER.error("Writing {} document(s) into collection [{}] failed -> remaining retries ({})",
                    writeModels.size(), namespace.getFullName(), getRemainingRetries(config).get());
            checkRetriableException(context, config, e);
        }
    }

    private MongoSinkHelper(){
    }
}
