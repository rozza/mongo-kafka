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
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class MongoDbUpdate implements CdcOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbUpdate.class);
	
	private static final String JSON_DOC_DOCUMENT_KEY_FIELD_PATH = "documentKey";
	private static final String JSON_DOC_UPDATE_DESCRIPTION_FIELD_PATH = "updateDescription";
    private static final String UPDATE_DESCRIPTION_UPDATED_FIELDS_FIELD_PATH = "updatedFields";
    private static final String UPDATE_DESCRIPTION_REMOVED_FIELDS_FIELD_PATH = "removedFields";
    
    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("Error: value doc must not be missing for update operation")
        );

        try {
	        	LOGGER.info("UPDATE OPERATION: SinkDocument - Key: " + doc.getKeyDoc().toString());
	        	LOGGER.info("UPDATE OPERATION: SinkDocument - Value: " + doc.getValueDoc().toString());
	        	
	        	// Here we get the full document in extended JSON format.
	        	// As value.converter=com.mongodb.kafka.connect.sink.converter.JsonRawStringRecordConverter cannot be used due to classloader isolation
	        	// ?? What can we do about it ??
	        	
	        	// Therefore, we need to create a text of the valueDoc and use BsonDocument.parse to parse it.
	        	// FIXME: This is double parsing and needs to be avoided.
	        	
	        	BsonDocument updateDescriptionDoc = valueDoc.getDocument(JSON_DOC_UPDATE_DESCRIPTION_FIELD_PATH);
	        	BsonDocument updatedFields = null;
	        	try { 
	        		updatedFields = updateDescriptionDoc.getDocument(UPDATE_DESCRIPTION_UPDATED_FIELDS_FIELD_PATH);
	        	}
	        	catch(BsonInvalidOperationException bioe) {}
	        	
	        	BsonArray removedFields = null;
	        	try { 
	        		removedFields = updateDescriptionDoc.getArray(UPDATE_DESCRIPTION_REMOVED_FIELDS_FIELD_PATH);
	        	}
	        	catch(BsonInvalidOperationException bioe) {}
	        	
	        	if (updatedFields != null || removedFields != null) {
	        			BsonDocument updateDocument = new BsonDocument();
		        	if (updatedFields != null) {
		        		updateDocument.append("$set", BsonDocument.parse(updatedFields.toJson()));
		        	}
		        	
		        	if (removedFields != null && removedFields.size() > 0) {
		        		BsonDocument unsetDocument = new BsonDocument();
		        		updateDocument.append("$unset", unsetDocument);
		        		for (int i = 0; i < removedFields.size(); i++) {
		        			BsonString field = removedFields.get(i).asString();
		        			unsetDocument.append(field.getValue(), new BsonString(""));
		        		}
		        	}
		        	LOGGER.info("UPDATE OPERATION: Filter Document: " + BsonDocument.parse(valueDoc.getDocument(JSON_DOC_DOCUMENT_KEY_FIELD_PATH).toJson()));
		        	LOGGER.info("UPDATE OPERATION: Update Document: " + updateDocument.toJson());
		        return new UpdateOneModel<BsonDocument>(BsonDocument.parse(valueDoc.getDocument(JSON_DOC_DOCUMENT_KEY_FIELD_PATH).toJson()), updateDocument, new UpdateOptions().upsert(true) );
	        	}
	        	else {
	        		throw new DataException("No modifications could be found. Change Events with operationType update always have either of them. Value Document: " + valueDoc.toJson());
	        	}
        } catch (Exception exc) {
            throw new DataException(exc);
        }

    }

}
