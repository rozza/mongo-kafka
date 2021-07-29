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
import java.util.stream.Collectors;

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
  private static final String MERGE_OBJECTS = "$mergeObjects";
  private static final BsonString VAL = new BsonString("$$val");

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
    BsonDocument updateDescription = getAndValidateUpdateDescription(changeStreamDocument);

    if (updateDescription.containsKey(TRUNCATED_ARRAYS)) {
      System.out.println(">>>>>>>>>>> " + changeStreamDocument.toJson());
      List<BsonDocument> updates = new ArrayList<>();
      addTruncations(updateDescription, updates);
      addUpdates(updateDescription, updates);
      addRemovals(updateDescription, updates);

      System.out.println(new BsonDocument("UPDATES: ", new BsonArray(updates)).toJson());
      return new UpdateOneModel<>(documentKey, updates);
    } else {
      BsonDocument update = new BsonDocument();
      addRemovals(updateDescription, update);
      addUpdates(updateDescription, update);
      return new UpdateOneModel<>(documentKey, update);
    }
  }

  private static BsonDocument getAndValidateUpdateDescription(
      final BsonDocument changeStreamDocument) {
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
    } else {
      for (final BsonValue removedField :
          updateDescription.getArray(REMOVED_FIELDS, new BsonArray())) {
        if (!removedField.isString()) {
          throw new DataException(
              format(
                  "Unexpected value type in %s, expected an string but found `%s`: %s",
                  REMOVED_FIELDS, removedField, updateDescription.toJson()));
        }
      }
    }

    if (updateDescription.containsKey(TRUNCATED_ARRAYS)) {
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
        if (!(truncatedDocument.containsKey(TRUNCATED_ARRAY_FIELD)
                && truncatedDocument.isString(TRUNCATED_ARRAY_FIELD))
            || !(truncatedDocument.containsKey(TRUNCATED_ARRAY_SIZE)
                && truncatedDocument.isInt32(TRUNCATED_ARRAY_SIZE))) {

          throw new DataException(
              format(
                  "Unexpected format in %s, expected valid '%s' and '%s' values but found: `%s`.\n%s",
                  TRUNCATED_ARRAYS,
                  TRUNCATED_ARRAY_FIELD,
                  TRUNCATED_ARRAY_SIZE,
                  truncatedDocument.toJson(),
                  updateDescription.toJson()));
        }
      }
    }

    return updateDescription;
  }

  private static void addTruncations(
      final BsonDocument updateDescription, final List<BsonDocument> updates) {
    BsonDocument truncations = new BsonDocument();
    for (final BsonValue truncatedValue :
        updateDescription.getArray(TRUNCATED_ARRAYS, new BsonArray())) {
      BsonDocument truncatedDocument = truncatedValue.asDocument();
      String fieldName = truncatedDocument.getString(TRUNCATED_ARRAY_FIELD).getValue();
      int newSize = truncatedDocument.getInt32(TRUNCATED_ARRAY_SIZE).intValue();
      truncations.append(
          fieldName,
          new BsonDocument(
              "$slice",
              new BsonArray(
                  asList(
                      new BsonString(format("$%s", fieldName)),
                      new BsonInt32(0),
                      new BsonInt32(newSize)))));
    }

    if (!truncations.isEmpty()) {
      updates.add(new BsonDocument(SET, truncations));
    }
  }

  private static BsonDocument addRemovals(
      final BsonDocument updateDescription, final BsonDocument removalDocument) {
    BsonDocument unsetDocument = new BsonDocument();
    for (final BsonValue removedField :
        updateDescription.getArray(REMOVED_FIELDS, new BsonArray())) {
      unsetDocument.append(removedField.asString().getValue(), EMPTY_STRING);
    }
    if (!unsetDocument.isEmpty()) {
      removalDocument.put(UNSET, unsetDocument);
    }
    return removalDocument;
  }

  private static void addRemovals(
      final BsonDocument updateDescription, final List<BsonDocument> updates) {
    BsonArray removals = new BsonArray();
    for (final BsonValue removedField :
        updateDescription.getArray(REMOVED_FIELDS, new BsonArray())) {
      removals.add(removedField.asString());
    }
    if (!removals.isEmpty()) {
      updates.add(new BsonDocument(UNSET, removals));
    }
  }

  private static BsonDocument addUpdates(
      final BsonDocument updateDescription, final BsonDocument updates) {
    BsonDocument updateDocument = updateDescription.getDocument(UPDATED_FIELDS);
    if (!updateDocument.isEmpty()) {
      updates.put(SET, updateDocument);
    }
    return updates;
  }

  private static final String UPDATE_TRUNCATED_ARRAY_TEMPLATE =
      ""
          + "{'%s': {" // FIELD PATH
          + "  $map: {"
          // Convert the array to a set of pairs [value, index in the array]"
          + "    input: {$zip: {inputs: ['$%s', {$range: [0, {$size: '$%s'}]}]}},"
          + "    in: {"
          + "        $let: {"
          // Expand the [value, index in the array] pairs into variables for each one.
          + "            vars: {"
          + "                val: {$arrayElemAt: ['$$this', 0]},"
          + "                idx: {$arrayElemAt: ['$$this', 1]}"
          + "            },"
          + "            in: {"
          + "              $cond: {"
          + "                if: {$eq: ['$$idx', %s]}," // INDEX
          + "                then: %s," // NEW VALUE MERGE OBJECTS
          + "                else: '$$val'"
          + "              }"
          + "            }"
          + "        }"
          + "    }"
          + "  }"
          + "}}";

  private static void addUpdates(
      final BsonDocument updateDescription, final List<BsonDocument> updates) {
    BsonDocument updatedFieldsDocument = updateDescription.getDocument(UPDATED_FIELDS);
    List<String> truncations =
        updateDescription.getArray(TRUNCATED_ARRAYS, new BsonArray()).stream()
            .map(b -> b.asDocument().getString(TRUNCATED_ARRAY_FIELD).getValue())
            .collect(Collectors.toList());

    Set<String> truncatedUpdates =
        updatedFieldsDocument.keySet().stream()
            .filter(k -> truncations.stream().anyMatch(k::startsWith))
            .collect(Collectors.toSet());

    Set<String> nonTruncatedUpdates = new HashSet<>(updatedFieldsDocument.keySet());
    nonTruncatedUpdates.removeAll(truncatedUpdates);

    if (!nonTruncatedUpdates.isEmpty()) {
      BsonDocument updateDocument = new BsonDocument();
      nonTruncatedUpdates.forEach(k -> updateDocument.put(k, updatedFieldsDocument.get(k)));
      updates.add(new BsonDocument(SET, updateDocument));
    }

    if (!truncatedUpdates.isEmpty()) {
      truncatedUpdates.forEach(
          k -> {
            String[] parts = k.split("\\.");
            int lastPositionIndex = parts.length;
            int indexPos = -1;
            List<String> fieldNameParts = new ArrayList<>();
            List<String> subFieldParts = new ArrayList<>();

            while (lastPositionIndex > 0) {
              String current = parts[--lastPositionIndex];
              if (indexPos != -1) {
                fieldNameParts.add(current);
              } else {
                try {
                  indexPos = Integer.parseInt(current);
                } catch (NumberFormatException e) {
                  subFieldParts.add(current);
                }
              }
            }
            String fieldPath = String.join(".", fieldNameParts);
            String fieldName = fieldNameParts.get(fieldNameParts.size() - 1);
            String subField = String.join(".", subFieldParts);
            BsonValue newValue =
                subField.isEmpty()
                    ? updatedFieldsDocument.get(k)
                    : new BsonDocument(subField, updatedFieldsDocument.get(k));
            String mergeObjectsJson =
                new BsonDocument(MERGE_OBJECTS, new BsonArray(asList(VAL, newValue))).toJson();

            System.out.println(
                new BsonDocument(
                    SET,
                    BsonDocument.parse(
                        format(
                            UPDATE_TRUNCATED_ARRAY_TEMPLATE,
                            fieldPath,
                            fieldPath,
                            fieldPath,
                            indexPos,
                            mergeObjectsJson))));
            updates.add(
                new BsonDocument(
                    SET,
                    BsonDocument.parse(
                        format(
                            UPDATE_TRUNCATED_ARRAY_TEMPLATE,
                            fieldPath,
                            fieldPath,
                            fieldPath,
                            indexPos,
                            mergeObjectsJson))));
          });
    }
  }

  private OperationHelper() {}
}
