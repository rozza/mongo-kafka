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

import com.mongodb.MongoNamespace;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import static java.lang.String.format;

class OperationHelper {

    private static final String DOCUMENT_KEY = "documentKey";
    private static final String FULL_DOCUMENT = "fullDocument";
    private static final String UPDATE_DESCRIPTION = "updateDescription";
    private static final String UPDATED_FIELDS = "updatedFields";
    private static final String REMOVED_FIELDS = "removedFields";
    private static final String SET = "$set";
    private static final String UNSET = "$unset";
    private static final BsonString EMPTY_STRING = new BsonString("");

    private static final String NS = "ns";
    private static final String DATABASE_NAME = "db";
    private static final String COLLECTION_NAME = "coll";

    static BsonDocument getDocumentKey(final BsonDocument changeStreamDocument) {
        if (!changeStreamDocument.containsKey(DOCUMENT_KEY)) {
            throw new DataException(format("Missing %s field: %s", DOCUMENT_KEY, changeStreamDocument.toJson()));
        } else if (!changeStreamDocument.get(DOCUMENT_KEY).isDocument()) {
            throw new DataException(format("Unexpected %s field type, expecting a document but found `%s`: %s", DOCUMENT_KEY,
                    changeStreamDocument.get(DOCUMENT_KEY), changeStreamDocument.toJson()));
        }

        return changeStreamDocument.getDocument(DOCUMENT_KEY);
    }

    static boolean hasFullDocument(final BsonDocument changeStreamDocument){
        return changeStreamDocument.containsKey(FULL_DOCUMENT);
    }

    static BsonDocument getFullDocument(final BsonDocument changeStreamDocument) {
        if (!changeStreamDocument.containsKey(FULL_DOCUMENT)) {
            throw new DataException(format("Missing %s field: %s", FULL_DOCUMENT, changeStreamDocument.toJson()));
        } else if (!changeStreamDocument.get(FULL_DOCUMENT).isDocument()) {
            throw new DataException(format("Unexpected %s field type, expecting a document but found `%s`: %s", FULL_DOCUMENT,
                    changeStreamDocument.get(FULL_DOCUMENT), changeStreamDocument.toJson()));
        }

        return changeStreamDocument.getDocument(FULL_DOCUMENT);
    }

    static BsonDocument getUpdateDocument(final BsonDocument changeStreamDocument) {
        if (!changeStreamDocument.containsKey(UPDATE_DESCRIPTION)) {
            throw new DataException(format("Missing %s field: %s", UPDATE_DESCRIPTION, changeStreamDocument.toJson()));
        } else if (!changeStreamDocument.get(UPDATE_DESCRIPTION).isDocument()) {
            throw new DataException(format("Unexpected %s field type, expected a document found `%s`: %s", UPDATE_DESCRIPTION,
                    changeStreamDocument.get(UPDATE_DESCRIPTION) ,changeStreamDocument.toJson()));
        }

        BsonDocument updateDescription = changeStreamDocument.getDocument(UPDATE_DESCRIPTION);

        if (!updateDescription.containsKey(UPDATED_FIELDS)) {
            throw new DataException(format("Missing %s.%s field: %s", UPDATE_DESCRIPTION, UPDATED_FIELDS, changeStreamDocument.toJson()));
        } else if (!updateDescription.get(UPDATED_FIELDS).isDocument()) {
            throw new DataException(format("Unexpected %s field type, expected a document but found `%s`: %s", UPDATE_DESCRIPTION,
                    changeStreamDocument.get(UPDATE_DESCRIPTION), changeStreamDocument.toJson()));
        }

        if (!updateDescription.containsKey(REMOVED_FIELDS)) {
            throw new DataException(format("Missing %s.%s field: %s", UPDATE_DESCRIPTION, REMOVED_FIELDS, changeStreamDocument.toJson()));
        } else if (!updateDescription.get(REMOVED_FIELDS).isArray()) {
            throw new DataException(format("Unexpected %s field type, expected an array but found `%s`: %s", REMOVED_FIELDS,
                    changeStreamDocument.get(REMOVED_FIELDS), changeStreamDocument.toJson()));
        }

        BsonDocument updatedFields = updateDescription.getDocument(UPDATED_FIELDS);
        BsonArray removedFields = updateDescription.getArray(REMOVED_FIELDS);
        BsonDocument unsetDocument = new BsonDocument();
        for (final BsonValue removedField : removedFields) {
            if (!removedField.isString()) {
                throw new DataException(format("Unexpected value type in %s, expected an string but found `%s`: %s", REMOVED_FIELDS,
                        removedField, changeStreamDocument.toJson()));
            }
            unsetDocument.append(removedField.asString().getValue(), EMPTY_STRING);
        }

        BsonDocument updateDocument = new BsonDocument(SET, updatedFields);
        if (!unsetDocument.isEmpty()) {
            updateDocument.put(UNSET, unsetDocument);
        }

        return updateDocument;
    }

    static MongoNamespace getNamespace(final BsonDocument changeStreamDocument) {
        return new MongoNamespace(getDatabaseName(changeStreamDocument), getCollectionName(changeStreamDocument));
    }

    static MongoNamespace getNamespace(final BsonDocument changeStreamDocument, final String fieldName) {
        return new MongoNamespace(getDatabaseName(changeStreamDocument, fieldName), getCollectionName(changeStreamDocument, fieldName));
    }

    static String getDatabaseName(final BsonDocument changeStreamDocument) {
        return getSubFieldString(changeStreamDocument, NS, DATABASE_NAME);
    }

    static String getCollectionName(final BsonDocument changeStreamDocument) {
        return getSubFieldString(changeStreamDocument, NS, COLLECTION_NAME);
    }

    static String getDatabaseName(final BsonDocument changeStreamDocument, final String fieldName) {
        return getSubFieldString(changeStreamDocument, fieldName, DATABASE_NAME);
    }

    static String getCollectionName(final BsonDocument changeStreamDocument, final String fieldName) {
        return getSubFieldString(changeStreamDocument, fieldName, COLLECTION_NAME);
    }

    static String getSubFieldString(final BsonDocument document, final String topLevelFieldName, final String fieldName) {
        if (!document.containsKey(topLevelFieldName)) {
            throw new DataException(format("Missing `%s.%s` field: %s in", topLevelFieldName, fieldName, document.toJson()));
        } else if (!document.get(topLevelFieldName).isDocument()) {
            throw new DataException(format("Unexpected `%s` field type, expecting a document but found `%s`: %s", topLevelFieldName,
                    document.get(fieldName), document.toJson()));
        }

        BsonDocument subDocument = document.getDocument(fieldName);
        if (!subDocument.get(fieldName).isString()) {
            throw new DataException(format("Unexpected `%s.%s` field type, expecting a string but found `%s`: %s", topLevelFieldName,
                    fieldName, subDocument.get(fieldName), document.toJson()));
        }

        return subDocument.getString(fieldName).getValue();
    }


    private OperationHelper() {
    }
}
