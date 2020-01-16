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

package com.mongodb.kafka.connect.sink.cdc.debezium;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.kafka.connect.sink.MongoSinkHelper.processWriteModels;
import static java.lang.String.format;

public abstract class DebeziumCdcHandler extends CdcHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumCdcHandler.class);
    private static final SinkConverter SINK_CONVERTER = new SinkConverter();
    private static final String OPERATION_TYPE = "op";

    private final Map<OperationType, CdcOperation> operations = new HashMap<>();

    public DebeziumCdcHandler() {
    }

    public abstract Optional<WriteModel<BsonDocument>> handle(SinkDocument doc);

    protected void registerOperations(final Map<OperationType, CdcOperation> operations) {
        this.operations.putAll(operations);
    }

    @Override
    public void processSinkRecords(final SinkTaskContext context, final MongoSinkTopicConfig config, final MongoClient mongoClient, 
                                   final List<SinkRecord> batch) {
        processWriteModels(context, config, mongoClient, buildWriteModels(config, batch));
    }

    List<? extends WriteModel<BsonDocument>> buildWriteModels(final MongoSinkTopicConfig config, final List<SinkRecord> batch) {
        LOGGER.debug("Building CDC write model for {} record(s) for topic {}", batch.size(), config.getTopic());
        return batch.stream()
                .map(SINK_CONVERTER::convert)
                .map(this::handle)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
    }

    public CdcOperation getCdcOperation(final BsonDocument doc) {
        if (!doc.containsKey(OPERATION_TYPE)) {
            throw new DataException(format("Error: `%s` field is doc is missing. %s", OPERATION_TYPE, doc.toJson()));
        } else if (!doc.get(OPERATION_TYPE).isString()) {
            throw new DataException("Error: Unexpected CDC operation type, should be a string");
        }

        CdcOperation op;
        try {
            op = operations.get(OperationType.fromString(doc.get(OPERATION_TYPE).asString().getValue()));
        } catch (IllegalArgumentException exc) {
            throw new DataException("Error: parsing CDC operation failed", exc);
        }

        if (op == null) {
            throw new DataException(format("Error: no CDC operation found in mapping for op=%s",
                    doc.get(OPERATION_TYPE).asString().getValue()));
        }

        return op;
    }

}
