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

package com.mongodb.kafka.connect.sink.cdc.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Delete;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.DropCollection;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.DropDatabase;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Invalidate;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.RenameCollection;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Replace;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Unknown;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Update;
import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.kafka.connect.sink.MongoSinkHelper.processWriteModels;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

public final class ChangeStreamHandler extends CdcHandler {
    private static final SinkConverter SINK_CONVERTER = new SinkConverter();
    private static final String OPERATION_TYPE = "operationType";
    private static final Map<OperationType, MongoCdcOperation> OPERATIONS = unmodifiableMap(
            new HashMap<OperationType, MongoCdcOperation>() {{
                put(OperationType.INSERT, new Replace());
                put(OperationType.REPLACE, new Replace());
                put(OperationType.UPDATE, new Update());
                put(OperationType.DELETE, new Delete());
                put(OperationType.DROP_COLLECTION, new DropCollection());
                put(OperationType.DROP_DATABASE, new DropDatabase());
                put(OperationType.RENAME_COLLECTION, new RenameCollection());
                put(OperationType.INVALIDATE, new Invalidate());
                put(OperationType.UNKNOWN, new Unknown());
            }}
    );
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamHandler.class);

    public ChangeStreamHandler() {
    }

    @Override
    public void processSinkRecords(final SinkTaskContext context, final MongoSinkTopicConfig config,
                                   final MongoClient mongoClient, final List<SinkRecord> batch) {
        List<Either<? extends WriteModel<BsonDocument>, Consumer<MongoClient>>> processedData = processSinkRecords(config, batch);

        List<WriteModel<BsonDocument>> writeModels = new ArrayList<>();
        for (Either<? extends WriteModel<BsonDocument>, Consumer<MongoClient>> either : processedData) {
            either.apply((Consumer<WriteModel<BsonDocument>>) writeModels::add, consumer -> {
                processWriteModels(context, config, mongoClient, writeModels);
                writeModels.clear();
                consumer.accept(mongoClient);
            });
        }
        processWriteModels(context, config, mongoClient, writeModels);
    }

    List<Either<? extends WriteModel<BsonDocument>, Consumer<MongoClient>>> processSinkRecords(final MongoSinkTopicConfig config,
                                                                                     final Collection<SinkRecord> batch) {
        LOGGER.debug("Processing CDC data for {} record(s) for topic {}", batch.size(), config.getTopic());
        return batch.stream()
                .map(SINK_CONVERTER::convert)
                .map(this::handle)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
    }

    Optional<Either<WriteModel<BsonDocument>, Consumer<MongoClient>>> handle(final SinkDocument doc) {
        BsonDocument changeStreamDocument = doc.getValueDoc().orElseGet(BsonDocument::new);

        if (!changeStreamDocument.containsKey(OPERATION_TYPE)) {
            throw new DataException(format("Error: `%s` field is doc is missing. %s", OPERATION_TYPE, changeStreamDocument.toJson()));
        } else if (!changeStreamDocument.get(OPERATION_TYPE).isString()) {
            throw new DataException("Error: Unexpected CDC operation type, should be a string");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating operation handler for: {}", OPERATION_TYPE);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("ChangeStream document {}", changeStreamDocument.toJson());
        }

        return Optional.of(
                OPERATIONS.get(OperationType.fromString(changeStreamDocument.get(OPERATION_TYPE).asString().getValue()))
                        .process(doc)
        );
    }
}
