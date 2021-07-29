package com.mongodb.kafka.connect;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

import com.mongodb.kafka.connect.mongodb.MongoDBHelper;

public class ArrayUpdateTestOld {

  @RegisterExtension public static final MongoDBHelper MONGODB = new MongoDBHelper();

  @Test
  void testArrayUpdate() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, arr: [1, 3]}"));
    test.insertOne(Document.parse("{_id: 2, arr: [1, 3]}"));
    test.updateOne(Document.parse("{_id: 1}"), Document.parse("{$set: {'arr.0': 2}}"));

    test.updateOne(
        Document.parse("{_id: 2}"),
        singletonList(
            Document.parse(
                "{$set: "
                    + "{arr: {" // FIELD PATH
                    + "  $map: {"
                    // Convert the array to a set of pairs [value, index in the array]"
                    + "    input: {$zip: {inputs: ['$arr', {$range: [0, {$size: '$arr'}]}]}},"
                    + "    in: {"
                    + "        $let: {"
                    // Expand the [value, index in the array] pairs into variables for each one.
                    + "            vars: {"
                    + "                val: {$arrayElemAt: ['$$this', 0]},"
                    + "                idx: {$arrayElemAt: ['$$this', 1]}"
                    + "            },"
                    + "            in: {"
                    + "              $cond: {"
                    + "                if: {$eq: ['$$idx', 0]}," // INDEX
                    + "                then: 2," // NEW VALUE MERGE OBJECTS
                    + "                else: '$$val'"
                    + "              }"
                    + "            }"
                    + "        }"
                    + "    }"
                    + "  }"
                    + "}"
                    + "}"
                    + "}")));

    List<Document> actual = test.find().into(new ArrayList<>());
    List<Document> expected =
        asList(Document.parse("{_id: 1, arr: [2, 3]}"), Document.parse("{_id: 2, arr: [2, 3]}"));

    assertIterableEquals(expected, actual);
  }

  @Test
  void testNestedArrayUpdate() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, arr: [[1, 3]]}"));
    test.insertOne(Document.parse("{_id: 2, arr: [[1, 3]]}"));
    test.updateOne(Document.parse("{_id: 1}"), Document.parse("{$set: {'arr.0.0': 2}}"));

    String mapTemplate =
        "{$map: {"
            + "input: {$cond: [{$isArray: ['$$l0']}, '$$l0', ['$$l0']]}," // FieldName
            + "as: 'l1',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$$l0']}, {$eq: [{ $indexOfArray: ['$$l0', '$$l1'] }, 0]}] }, " // FieldName //path part
            + "   then: 2," // Process array element $map and down we go
            + "   else: '$$l1'" // Process Document $map and down we go // TODO
            + "  }}}}   ";

    String nestedSet = mapTemplate;

    String updateJson =
        "{$set: {"
            + "'arr': {" // Fieldname
            + "$map: {"
            + "input: {$cond: [{$isArray: ['$arr']}, '$arr', ['$arr']]},"
            + "as: 'l0',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$arr']}, {$eq: [{ $indexOfArray: ['$arr', '$$l0'] }, 0]}] }, " // FieldName //path part
            + "   then: "
            + nestedSet
            + "," // Process array element $map and down we go
            + "   else: '$$l0'" // Process Document $map and down we go // TODO
            + "  }   "
            + "}}}}}";

    test.updateOne(Document.parse("{_id: 2}"), singletonList(Document.parse(updateJson)));

    List<Document> actual = test.find().sort(Sorts.ascending("_id")).into(new ArrayList<>());
    List<Document> expected =
        asList(
            Document.parse("{_id: 1, arr: [[2, 3]]}"), Document.parse("{_id: 2, arr: [[2, 3]]}"));

    assertIterableEquals(expected, actual);
  }

  @Test
  void testNestedDocumentArrayUpdateOri() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, arr: {'0': [1, 3]}}"));
    test.insertOne(Document.parse("{_id: 2, arr: {'0': [1, 3]}}"));
    test.updateOne(Document.parse("{_id: 1}"), Document.parse("{$set: {'arr.0.0': 2}}"));

    String mapArrayTemplate =
        "{$map: {"
            + "input: { $cond: [{$isArray: ['$$l0']}, '$$l0', ['$$l0']]}," // FieldName
            + "as: 'l1',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$$l0']}, {$eq: [{ $indexOfArray: ['$$l0', '$$l1'] }, 0]}] }, " // FieldName //path part
            + "   then: 2," // Process array element $map and down we go
            + "   else: '$$l1'" // Process Document $map and down we go // TODO
            + "  }}}}   ";

    String nestedArraySet = mapArrayTemplate;

    String map =
        "{$map: {"
            + "input: { $cond: [{$isArray: ['$arr']}, '$arr', ['$arr']]},"
            + "as: 'l0',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$arr']}, {$eq: [{ $indexOfArray: ['$arr', '$$l0'] }, 0]}] }, " // FieldName //path part
            + "   then: "
            + nestedArraySet
            + "," // Process array element $map and down we go
            + "   else: {$setField: {field: '0', input: '$$l0', value: { $let: "
            + "                                                      { vars: {l0: {$getField: {field: '0', input: '$$l0'}}}, "
            + "                                                        in: "
            + nestedArraySet
            + "}}}}"
            + "}}}}"; // cond

    String firstFromMap = "{$first: " + map + " }";

    String updateJson =
        "{$set: {"
            + "'arr': " // Fieldname
            + "{$cond: [{$isArray: ['$arr']}, "
            + map
            + ", "
            + firstFromMap
            + "]}"
            + "}}}";

    System.out.println(updateJson);

    test.updateOne(Document.parse("{_id: 2}"), singletonList(Document.parse(updateJson)));

    List<Document> actual = test.find().into(new ArrayList<>());
    List<Document> expected =
        asList(
            Document.parse("{_id: 1, arr: {'0': [2, 3]}}}"),
            Document.parse("{_id: 2, arr: {'0': [2, 3]}}"));

    for (int i = 0; i < actual.size(); i++) {
      System.out.println(actual.get(i).toJson());
    }

    assertIterableEquals(expected, actual);
  }

  @Test
  void testNestedDocumentArrayUpdate() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, l: {'0': [1, 3]}}"));
    test.insertOne(Document.parse("{_id: 2, l: [[1, 3]]}"));
    test.insertOne(Document.parse("{_id: 3, l: {'0': [1, 3]}}"));
    test.insertOne(Document.parse("{_id: 4, l: [[1, 3]]}"));
    test.updateOne(Document.parse("{_id: 1}"), Document.parse("{$set: {'l.0.0': 2}}"));
    test.updateOne(Document.parse("{_id: 2}"), Document.parse("{$set: {'l.0.0': 2}}"));

    String nestedArraySet =
        "{$map: {"
            + "input: { $cond: [{$isArray: ['$$l0']}, '$$l0', ['$$l0']]}," // FieldName
            + "as: 'l1',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$$l0']}, {$eq: [{ $indexOfArray: ['$$l0', '$$l1'] }, 0]}] }, " // FieldName //path part
            + "   then: 2," // Process lay element $map and down we go
            + "   else: '$$l1'" // Process Document $map and down we go // TODO
            + "  }}}}   ";

    String map =
        "{$map: {"
            + "input: { $cond: [{$isArray: ['$l']}, '$l', ['$l']]},"
            + "as: 'l0',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$l']}, {$eq: [{ $indexOfArray: ['$l', '$$l0'] }, 0]}] }, " // FieldName //path part
            + "   then: "
            + nestedArraySet
            + "," // Process lay element $map and down we go
            + "   else: {$setField: {field: '0', input: '$$l0', value: { $let: "
            + "                                                      { vars: {l0: {$getField: {field: '0', input: '$$l0'}}}, "
            + "                                                        in: "
            + nestedArraySet
            + "}}}}"
            + "}}}}"; // cond

    String firstFromMap = "{$first: " + map + " }";

    String updateJson =
        "{$set: {"
            + "'l': " // Fieldname
            + "{$cond: [{$isArray: ['$l']}, "
            + map
            + ", "
            + firstFromMap
            + "]}"
            + "}}}";

    System.out.println(updateJson);

    test.updateOne(Document.parse("{_id: 3}"), singletonList(Document.parse(updateJson)));
    test.updateOne(Document.parse("{_id: 4}"), singletonList(Document.parse(updateJson)));

    List<Document> actual = test.find().into(new ArrayList<>());
    List<Document> expected =
        asList(
            Document.parse("{_id: 1, l: {'0': [2, 3]}}"),
            Document.parse("{_id: 2, l: [[2, 3]]}"),
            Document.parse("{_id: 3, l: {'0': [2, 3]}}}"),
            Document.parse("{_id: 4, l: [[2, 3]]}"));

    for (int i = 0; i < actual.size(); i++) {
      System.out.println(actual.get(i).toJson());
    }

    assertIterableEquals(expected, actual);
  }

  @Test
  void testNestedDocumentArrayUpdateLL() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, l: {l: {'0': [1, 3]}}}"));
    test.insertOne(Document.parse("{_id: 2, l: {l: [[1, 3]]}}"));
    test.insertOne(Document.parse("{_id: 3, l: {l: {'0': [1, 3]}}}"));
    test.insertOne(Document.parse("{_id: 4, l: {l: [[1, 3]]}}"));
    test.updateOne(Document.parse("{_id: 1}"), Document.parse("{$set: {'l.l.0.1': 2}}"));
    test.updateOne(Document.parse("{_id: 2}"), Document.parse("{$set: {'l.l.0.1': 2}}"));

    String map =
        "{$map: {"
            + "input: { $cond: [{$isArray: ['$l.l']}, '$l.l', ['$l.l']]},"
            + "as: 'l0',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$l.l']}, {$eq: [{ $indexOfArray: ['$l.l', '$$l0'] }, 0]}] }, " // FieldName //path part
            + "   then: "
            + "{$map: {"
            + "input: { $cond: [{$isArray: ['$$l0']}, '$$l0', ['$$l0']]}," // FieldName
            + "as: 'l1',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$$l0']}, {$eq: [{ $indexOfArray: ['$$l0', '$$l1'] }, 1]}] }, " // FieldName //path part
            + "   then: 2," // Process lay element $map and down we go
            + "   else: '$$l1'" // Process Document $map and down we go // TODO
            + "  }}}}   "
            + "," // Process lay element $map and down we go
            + "   else: {$setField: {field: '0', input: '$$l0', value: { $let: { vars: {l0: '$$l0.0'}, in: "
            + "{$map: {"
            + "input: { $cond: [{$isArray: ['$$l0']}, '$$l0', ['$$l0']]}," // FieldName
            + "as: 'l1',"
            + "in: { "
            + "  $cond: {"
            + "   if: { $and: [{$isArray: ['$$l0']}, {$eq: [{ $indexOfArray: ['$$l0', '$$l1'] }, 1]}] }, " // FieldName //path part
            + "   then: 2," // Process lay element $map and down we go
            + "   else: '$$l1'"
            + "  }}}}   "
            + "}}}}"
            + "}}}}"; // cond

    System.out.println(
        BsonDocument.parse(map)
            .toJson(
                JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build()));

    String firstFromMap = "{$first: " + map + " }";

    String updateJson =
        "{$set: {"
            + "'l.l': " // Fieldname
            + "{$cond: [{$isArray: ['$l.l']}, "
            + map
            + ", "
            + firstFromMap
            + "]}"
            + "}}}";

    System.out.println(updateJson);

    test.updateOne(Document.parse("{_id: 3}"), singletonList(Document.parse(updateJson)));
    test.updateOne(Document.parse("{_id: 4}"), singletonList(Document.parse(updateJson)));

    List<Document> actual = test.find().into(new ArrayList<>());
    List<Document> expected =
        asList(
            Document.parse("{_id: 1, l: {l: {'0': [1, 2]}}}"),
            Document.parse("{_id: 2, l: {l: [[1, 2]]}}"),
            Document.parse("{_id: 3, l: {l: {'0': [1, 2]}}}}"),
            Document.parse("{_id: 4, l: {l: [[1, 2]]}}"));

    for (int i = 0; i < actual.size(); i++) {
      System.out.println(actual.get(i).toJson());
    }

    assertIterableEquals(expected, actual);
  }

  @Test
  void testGenerateUpdateAggregationStatement() {

    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    test.insertOne(Document.parse("{_id: 1, l: {l: {'0': [1, 3]}}}"));

    // test.updateOne(BsonDocument.parse("{_id: 1}"), generateUpdateAggregationStatement())
  }

  @Test
  void testFieldPathChunks() {

    assertEquals(
        singletonList(stringChunk("a")), splitPathIntoFieldNameAndPossibleArrayPositionChunks("a"));
    assertEquals(
        asList(stringChunk("a"), numberChunk("0")),
        splitPathIntoFieldNameAndPossibleArrayPositionChunks("a.0"));
    assertEquals(
        asList(stringChunk("a"), numberChunk("0"), numberChunk("0")),
        splitPathIntoFieldNameAndPossibleArrayPositionChunks("a.0.0"));
    assertEquals(
        asList(stringChunk("a.b.0a"), numberChunk("0")),
        splitPathIntoFieldNameAndPossibleArrayPositionChunks("a.b.0a.0"));
    assertEquals(
        asList(stringChunk("a.b"), numberChunk("0"), stringChunk("c"), numberChunk("0")),
        splitPathIntoFieldNameAndPossibleArrayPositionChunks("a.b.0.c.0"));
  }

  @Test
  void testGenerator() {
    BsonDocument updates = new BsonDocument("l.l.0.0", new BsonInt32(2));
    BsonDocument aggregationUpdates = generateUpdateAggregationUpdates(updates);
    System.out.println(
        ">> "
            + aggregationUpdates.toJson(
                JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build()));
  }

  @Test
  void testGenerated() {
    MongoDatabase original = MONGODB.getMongoClient().getDatabase("test");
    original.drop();
    MongoCollection<Document> test = original.getCollection("test");

    BsonDocument updates = new BsonDocument("l.l.0.1", new BsonInt32(2));

    test.insertOne(Document.parse("{_id: 1, l: {l: {'0': [1, 3]}}}"));
    test.insertOne(Document.parse("{_id: 2, l: {l: [[1, 3]]}}"));
    test.insertOne(Document.parse("{_id: 3, l: {l: {'0': [1, 3]}}}"));
    test.insertOne(Document.parse("{_id: 4, l: {l: [[1, 3]]}}"));
    test.updateOne(Document.parse("{_id: 1}"), new BsonDocument("$set", updates));
    test.updateOne(Document.parse("{_id: 2}"), new BsonDocument("$set", updates));

    BsonDocument aggregationUpdates = generateUpdateAggregationUpdates(updates);
    System.out.println(
        ">> "
            + aggregationUpdates.toJson(
                JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build()));

    test.updateOne(
        Document.parse("{_id: 3}"), singletonList(new BsonDocument("$set", aggregationUpdates)));
    test.updateOne(
        Document.parse("{_id: 4}"), singletonList(new BsonDocument("$set", aggregationUpdates)));

    List<Document> actual = test.find().into(new ArrayList<>());
    List<Document> expected =
        asList(
            Document.parse("{_id: 1, l: {l: {'0': [1, 2]}}}"),
            Document.parse("{_id: 2, l: {l: [[1, 2]]}}"),
            Document.parse("{_id: 3, l: {l: {'0': [1, 2]}}}}"),
            Document.parse("{_id: 4, l: {l: [[1, 2]]}}"));

    for (int i = 0; i < actual.size(); i++) {
      System.out.println(actual.get(i).toJson());
    }

    assertIterableEquals(expected, actual);

    System.out.println(
        ">> "
            + aggregationUpdates.toJson(
                JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build()));
  }

  BsonDocument isArray(final String fieldPath) {
    return BsonDocument.parse(format("{$isArray: ['$%s']}", fieldPath));
  }

  BsonDocument cond(
      final BsonDocument ifDocument, final BsonValue thenValue, final BsonValue elseValue) {
    return new BsonDocument("$cond", new BsonArray(asList(ifDocument, thenValue, elseValue)));
  }

  BsonDocument ifArrayPos(final String outerScope, final String innerScope, final String index) {
    return BsonDocument.parse(
        format(
            "{$and: [{$isArray: ['$%s']}, {$eq: [{$indexOfArray: ['$%s', '$$%s']}, %s]}]}",
            outerScope, outerScope, innerScope, index));
  }

  BsonDocument firstFrom(final BsonDocument stage) {
    return new BsonDocument("$first", stage);
  }

  BsonDocument setFieldPath(final String fieldPath, BsonDocument previousStage) {
    return new BsonDocument()
        .append(fieldPath, cond(isArray(fieldPath), previousStage, firstFrom(previousStage)));
  }

  BsonDocument setField(final String fieldPath, final String outerScope, final BsonValue inner) {
    return new BsonDocument()
        .append(
            "$setField",
            BsonDocument.parse(format("{field: '%s', input: '$%s'}", fieldPath, outerScope))
                .append("value", inner));
  }

  BsonValue setFieldValue(final String outerScope, final String innerScope, final BsonValue inner) {
    return BsonDocument.parse(format("{ vars: {'%s': '$$%s'}}", innerScope, outerScope))
        .append("in", inner);
  }

  BsonDocument mapPath(final String outerScope, final String innerScope, final BsonValue inner) {
    return new BsonDocument(
        "$map",
        new BsonDocument()
            .append(
                "input",
                cond(
                    isArray(outerScope),
                    new BsonString("$" + outerScope),
                    new BsonString("[$" + outerScope + "]")))
            .append("as", new BsonString(innerScope))
            .append("in", inner));
  }

  BsonDocument generateAggregationUpdate(
      final BsonValue newValue,
      final FieldPathChunk fieldPath,
      final FieldPathChunk possibleArrayIndex,
      final List<FieldPathChunk> subPaths) {

    System.out.println("fieldPath: " + fieldPath);
    System.out.println("possibleArray: " + possibleArrayIndex);
    System.out.println("subPaths: " + subPaths);

    BsonDocument aggregationStage = new BsonDocument();
    LinkedList<Function<BsonDocument, BsonDocument>> aggregationGenerator = new LinkedList<>();

    AtomicInteger atomicInteger = new AtomicInteger();
    aggregationGenerator.add((inner) -> setFieldPath(fieldPath.fieldPath, inner));

    boolean hasSubPaths = !subPaths.isEmpty();
    String outerScope = fieldPath.fieldPath;
    String innerScope = "l" + atomicInteger.getAndIncrement();

    aggregationGenerator.add((inner) -> mapPath(outerScope, innerScope, inner));
    aggregationGenerator.add(
        (inner) ->
            cond(
                ifArrayPos(outerScope, innerScope, possibleArrayIndex.fieldPath),
                inner,
                setField(
                    innerScope,
                    possibleArrayIndex.fieldPath,
                    subPaths.isEmpty()
                        ? inner
                        : setFieldValue(
                            format("%s.%s", innerScope, possibleArrayIndex.fieldPath),
                            innerScope,
                            inner))));

    if (!subPaths.isEmpty()) {
      while (subPaths.size() > 1) {
        FieldPathChunk nextPath = subPaths.remove(0);

        String subPathOuterScope = "$l" + atomicInteger.get();
        String subPathInnerScope = "$l" + atomicInteger.getAndIncrement();

        aggregationGenerator.add((inner) -> mapPath(outerScope, innerScope, inner));

        if (nextPath.isNumeric) {
          aggregationGenerator.add(
              (inner) ->
                  mapPath(
                      subPathOuterScope,
                      subPathInnerScope,
                      cond(
                          ifArrayPos(subPathOuterScope, subPathInnerScope, nextPath.fieldPath),
                          inner,
                          setField(nextPath.fieldPath, subPathInnerScope, inner))));

        } else {
          aggregationGenerator.add(
              (inner) -> setField(nextPath.fieldPath, subPathOuterScope, inner));
        }
      }

      FieldPathChunk nextPath = subPaths.remove(0);

      String finalStageInnerScope = !hasSubPaths ? innerScope : "$l" + atomicInteger.get();
      String finalStageOuterScope =
          !hasSubPaths ? outerScope : "$l" + atomicInteger.decrementAndGet();

      if (nextPath.isNumeric) {

        aggregationGenerator.add(
            (inner) ->
                mapPath(
                    finalStageOuterScope,
                    finalStageInnerScope,
                    cond(
                        ifArrayPos(finalStageOuterScope, finalStageInnerScope, nextPath.fieldPath),
                        newValue,
                        setField(nextPath.fieldPath, finalStageInnerScope, newValue))));
      } else {
        aggregationGenerator.add(
            (inner) -> setField(nextPath.fieldPath, finalStageOuterScope, newValue));
      }
    }

    while (aggregationGenerator.size() > 0) {
      aggregationStage = aggregationGenerator.removeLast().apply(aggregationStage);
    }

    return aggregationStage;
  }

  BsonDocument generateUpdateAggregationUpdates(final BsonDocument updates) {
    BsonDocument aggregationUpdates = new BsonDocument();
    updates.forEach(
        (k, v) -> {
          List<FieldPathChunk> fieldPathChunks =
              splitPathIntoFieldNameAndPossibleArrayPositionChunks(k);

          if (fieldPathChunks.size() > 1) {
            BsonDocument bsonDocument =
                generateAggregationUpdate(
                    v, fieldPathChunks.remove(0), fieldPathChunks.remove(0), fieldPathChunks);
            aggregationUpdates.putAll(bsonDocument);
          } else {
            aggregationUpdates.put(k, v);
          }
        });

    return aggregationUpdates;
  }

  List<FieldPathChunk> splitPathIntoFieldNameAndPossibleArrayPositionChunks(
      final String fieldPath) {
    List<FieldPathChunk> chunks = new ArrayList<>();
    StringBuilder sb = new StringBuilder();

    for (final String s : fieldPath.split("\\.")) {
      if (isNumeric(s)) {
        if (sb.length() > 0) {
          chunks.add(new FieldPathChunk(sb.toString(), false));
          sb.setLength(0);
        }
        chunks.add(new FieldPathChunk(s, true));
      } else {
        if (sb.length() > 0) {
          sb.append(".");
        }
        sb.append(s);
      }
    }
    if (sb.length() > 0) {
      chunks.add(new FieldPathChunk(sb.toString(), false));
    }
    return chunks;
  }

  private boolean isNumeric(final String string) {
    return string.chars().allMatch(Character::isDigit);
  }

  FieldPathChunk stringChunk(final String s) {
    return new FieldPathChunk(s, false);
  }

  FieldPathChunk numberChunk(final String s) {
    return new FieldPathChunk(s, true);
  }

  private static class FieldPathChunk {
    private final String fieldPath;
    private final boolean isNumeric;

    FieldPathChunk(final String fieldPath, final boolean isNumeric) {
      this.fieldPath = fieldPath;
      this.isNumeric = isNumeric;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FieldPathChunk that = (FieldPathChunk) o;
      return isNumeric == that.isNumeric && Objects.equals(fieldPath, that.fieldPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldPath, isNumeric);
    }

    @Override
    public String toString() {
      return "FieldPathChunk{"
          + "fieldPath='"
          + fieldPath
          + '\''
          + ", isNumeric="
          + isNumeric
          + '}';
    }
  }
}
