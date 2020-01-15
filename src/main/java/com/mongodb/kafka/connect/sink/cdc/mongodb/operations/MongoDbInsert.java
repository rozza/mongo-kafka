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

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class MongoDbInsert implements CdcOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbInsert.class);
	
    private static final String JSON_DOC_FIELD_PATH = "fullDocument";

    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("Error: value doc must not be missing for insert operation")
        );

        try {
	        	LOGGER.info("INSERT OPERATION: SinkDocument - Key: " + doc.getKeyDoc().toString());
	        	LOGGER.info("INSERT OPERATION: SinkDocument - Value: " + doc.getValueDoc().toString());
	        	
	        	// Here we get the full document in extended JSON format.
	        	// As value.converter=com.mongodb.kafka.connect.sink.converter.JsonRawStringRecordConverter cannot be used due to classloader isolation
	        	// ?? What can we do about it ??
	        	
	        	// Therefore, we need to create a text of the valueDoc and use BsonDocument.parse to parse it.
	        	// FIXME: This is double parsing and needs to be avoided.
	        	LOGGER.info("INSERT OPERATION: Value Document as JSON: " + valueDoc.get(JSON_DOC_FIELD_PATH).asDocument().toJson()); 	
            BsonDocument insertDoc = BsonDocument.parse(valueDoc.get(JSON_DOC_FIELD_PATH).asDocument().toJson());
            LOGGER.info("INSERT OPERATION: BSONDocument: " + insertDoc.toString());
            return new InsertOneModel<BsonDocument>(insertDoc);
        } catch (Exception exc) {
            throw new DataException(exc);
        }

    }

}
