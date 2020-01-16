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

package com.mongodb.kafka.connect.sink.cdc.mongodb.operations;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.cdc.mongodb.Either;
import com.mongodb.kafka.connect.sink.cdc.mongodb.MongoCdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.mongodb.kafka.connect.sink.cdc.mongodb.operations.OperationHelper.getDocumentKey;
import static com.mongodb.kafka.connect.sink.cdc.mongodb.operations.OperationHelper.getFullDocument;
import static com.mongodb.kafka.connect.sink.cdc.mongodb.operations.OperationHelper.getUpdateDocument;
import static com.mongodb.kafka.connect.sink.cdc.mongodb.operations.OperationHelper.hasFullDocument;

public class Update implements MongoCdcOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(Update.class);

    @Override
    public Either<WriteModel<BsonDocument>, Consumer<MongoClient>> process(final SinkDocument doc) {
        BsonDocument changeStreamDocument = doc.getValueDoc().orElseThrow(
                () -> new DataException("Error: value doc must not be missing for update operation")
        );

        BsonDocument documentKey = getDocumentKey(changeStreamDocument);

        if (hasFullDocument(changeStreamDocument)) {
            LOGGER.debug("The full Document available, creating a replace operation.");
            return Either.left(new ReplaceOneModel<>(documentKey, getFullDocument(changeStreamDocument)));
        }

        LOGGER.debug("No full document field available, creating update operation.");
        return Either.left(new UpdateOneModel<>(documentKey, getUpdateDocument(changeStreamDocument), new UpdateOptions().upsert(true)));
    }

}
