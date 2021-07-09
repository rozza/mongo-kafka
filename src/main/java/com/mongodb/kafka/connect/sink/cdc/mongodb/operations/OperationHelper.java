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

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.client.model.UpdateOneModel;

final class OperationHelper {

  private static final String DOCUMENT_KEY = "documentKey";
  private static final String FULL_DOCUMENT = "fullDocument";
  private static final String UPDATE_DESCRIPTION = "updateDescription";
  private static final String UPDATED_FIELDS = "updatedFields";
  private static final String REMOVED_FIELDS = "removedFields";
  private static final String TRUNCATED_ARRAYS = "truncatedArrays";
  private static final String TRUNCATED_ARRAY_FIELD = "field";
  private static final String TRUNCATED_ARRAY_SIZE = "newSize";

  private static final Set<String> UPDATE_DESCRIPTION_FIELDS =
      new HashSet<>(asList(UPDATED_FIELDS, REMOVED_FIELDS, TRUNCATED_ARRAYS));

  private static final String SET = "$set";
  private static final String UNSET = "$unset";
  private static final BsonString EMPTY_STRING = new BsonString("");

  static BsonDocument getDocumentKey(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(DOCUMENT_KEY)) {
      throw new DataException(
          format("Missing %s field: %s", DOCUMENT_KEY, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(DOCUMENT_KEY).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expecting a document but found `%s`: %s",
              DOCUMENT_KEY, changeStreamDocument.get(DOCUMENT_KEY), changeStreamDocument.toJson()));
    }

    return changeStreamDocument.getDocument(DOCUMENT_KEY);
  }

  static boolean hasFullDocument(final BsonDocument changeStreamDocument) {
    return changeStreamDocument.containsKey(FULL_DOCUMENT);
  }

  static BsonDocument getFullDocument(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(FULL_DOCUMENT)) {
      throw new DataException(
          format("Missing %s field: %s", FULL_DOCUMENT, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(FULL_DOCUMENT).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expecting a document but found `%s`: %s",
              FULL_DOCUMENT,
              changeStreamDocument.get(FULL_DOCUMENT),
              changeStreamDocument.toJson()));
    }

    return changeStreamDocument.getDocument(FULL_DOCUMENT);
  }

  static UpdateOneModel<BsonDocument> getUpdateOneModel(
      final BsonDocument documentKey, final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(UPDATE_DESCRIPTION)) {
      throw new DataException(
          format("Missing %s field: %s", UPDATE_DESCRIPTION, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(UPDATE_DESCRIPTION).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected a document found `%s`: %s",
              UPDATE_DESCRIPTION,
              changeStreamDocument.get(UPDATE_DESCRIPTION),
              changeStreamDocument.toJson()));
    }

    BsonDocument updateDescription = changeStreamDocument.getDocument(UPDATE_DESCRIPTION);
    Set<String> updateDescriptionFields = new HashSet<>(updateDescription.keySet());
    updateDescriptionFields.removeAll(UPDATE_DESCRIPTION_FIELDS);
    if (!updateDescriptionFields.isEmpty()) {
      throw new DataException(
          format(
              "Warning unexpected field(s) in %s %s. %s. Cannot process due to risk of data loss.",
              UPDATE_DESCRIPTION, updateDescriptionFields, updateDescription.toJson()));
    }

    if (updateDescription.containsKey(TRUNCATED_ARRAYS)) {
      BsonDocument setDocument = getArraySetDocument(updateDescription);
      BsonDocument unsetDocument = getUnsetDocument(updateDescription);
      BsonDocument truncatedArrayDocument = getTruncatedArrayDocument(updateDescription);
      List<BsonDocument> updates = new ArrayList<>();
      if (!truncatedArrayDocument.isEmpty()) {
        updates.add(new BsonDocument(SET, truncatedArrayDocument));
      }
      if (!unsetDocument.isEmpty()) {
        updates.add(new BsonDocument(UNSET, unsetDocument));
      }
      if (!setDocument.isEmpty()) {
        updates.add(new BsonDocument(SET, setDocument));
      }
      return new UpdateOneModel<>(documentKey, updates);
    } else {
      BsonDocument setDocument = getSetDocument(updateDescription);
      BsonDocument unsetDocument = getUnsetDocument(updateDescription);
      BsonDocument update = new BsonDocument();
      if (!unsetDocument.isEmpty()) {
        update.put(UNSET, unsetDocument);
      }
      if (!setDocument.isEmpty()) {
        update.put(SET, setDocument);
      }
      return new UpdateOneModel<>(documentKey, update);
    }
  }

  private static BsonDocument getArraySetDocument(final BsonDocument updateDescription) {
    if (!updateDescription.containsKey(UPDATED_FIELDS)) {
      throw new DataException(
          format(
              "Missing %s.%s field: %s",
              UPDATE_DESCRIPTION, UPDATED_FIELDS, updateDescription.toJson()));
    } else if (!updateDescription.get(UPDATED_FIELDS).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected a document but found `%s`: %s",
              UPDATE_DESCRIPTION, updateDescription, updateDescription.toJson()));
    }
    System.out.println(updateDescription.getDocument(UPDATED_FIELDS).toJson());
    return updateDescription.getDocument(UPDATED_FIELDS);
  }

  private static BsonDocument getSetDocument(final BsonDocument updateDescription) {
    if (!updateDescription.containsKey(UPDATED_FIELDS)) {
      throw new DataException(
          format(
              "Missing %s.%s field: %s",
              UPDATE_DESCRIPTION, UPDATED_FIELDS, updateDescription.toJson()));
    } else if (!updateDescription.get(UPDATED_FIELDS).isDocument()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected a document but found `%s`: %s",
              UPDATE_DESCRIPTION, updateDescription, updateDescription.toJson()));
    }
    return updateDescription.getDocument(UPDATED_FIELDS);
  }

  private static BsonDocument getUnsetDocument(final BsonDocument updateDescription) {
    if (!updateDescription.containsKey(REMOVED_FIELDS)) {
      throw new DataException(
          format(
              "Missing %s.%s field: %s",
              UPDATE_DESCRIPTION, REMOVED_FIELDS, updateDescription.toJson()));
    } else if (!updateDescription.get(REMOVED_FIELDS).isArray()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected an array but found `%s`: %s",
              REMOVED_FIELDS, updateDescription.get(REMOVED_FIELDS), updateDescription.toJson()));
    }

    BsonDocument unsetDocument = new BsonDocument();
    for (final BsonValue removedField :
        updateDescription.getArray(REMOVED_FIELDS, new BsonArray())) {
      if (!removedField.isString()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected an string but found `%s`: %s",
                REMOVED_FIELDS, removedField, updateDescription.toJson()));
      }
      unsetDocument.append(removedField.asString().getValue(), EMPTY_STRING);
    }
    return unsetDocument;
  }

  private static BsonDocument getTruncatedArrayDocument(final BsonDocument updateDescription) {
    BsonDocument truncatedArrayDocument = new BsonDocument();
    if (!updateDescription.containsKey(TRUNCATED_ARRAYS)) {
      return truncatedArrayDocument;
    }
    if (!updateDescription.get(TRUNCATED_ARRAYS).isArray()) {
      throw new DataException(
          format(
              "Unexpected %s field type, expected an array but found `%s`: %s",
              TRUNCATED_ARRAYS,
              updateDescription.get(TRUNCATED_ARRAYS),
              updateDescription.toJson()));
    }

    for (final BsonValue truncatedValue :
        updateDescription.getArray(TRUNCATED_ARRAYS, new BsonArray())) {
      if (!truncatedValue.isDocument()) {
        throw new DataException(
            format(
                "Unexpected value type in %s, expected an document but found `%s`: %s",
                TRUNCATED_ARRAYS, truncatedValue, updateDescription.toJson()));
      }
      BsonDocument truncatedDocument = truncatedValue.asDocument();
      if ((truncatedDocument.containsKey(TRUNCATED_ARRAY_FIELD)
              && truncatedDocument.isString(TRUNCATED_ARRAY_FIELD))
          && (truncatedDocument.containsKey(TRUNCATED_ARRAY_SIZE)
              && truncatedDocument.isInt32(TRUNCATED_ARRAY_SIZE))) {
        throw new DataException(
            format(
                "Unexpected format in %s, expected valid '%s' and '%s' field values but found `%s`: %s",
                TRUNCATED_ARRAYS,
                TRUNCATED_ARRAY_FIELD,
                TRUNCATED_ARRAY_SIZE,
                truncatedDocument.toJson(),
                updateDescription.toJson()));
      }
      String fieldName = truncatedDocument.getString(TRUNCATED_ARRAY_FIELD).getValue();
      int newSize = truncatedDocument.getInt32(TRUNCATED_ARRAY_SIZE).getValue();
      truncatedArrayDocument.append(
          fieldName,
          new BsonDocument(
              "$slice",
              new BsonArray(
                  asList(
                      new BsonString(format("$%s", fieldName)),
                      new BsonInt32(0),
                      new BsonInt32(newSize)))));
    }

    return truncatedArrayDocument;
  }

  private OperationHelper() {}
}
