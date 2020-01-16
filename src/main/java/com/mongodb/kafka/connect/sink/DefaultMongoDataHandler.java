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

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessors;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.mongodb.kafka.connect.sink.MongoSinkHelper.processWriteModels;

class DefaultMongoDataHandler implements MongoDataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMongoDataHandler.class);
    private static final SinkConverter SINK_CONVERTER = new SinkConverter();

    @Override
    public void processSinkRecords(final SinkTaskContext context, final MongoSinkTopicConfig config,
                                   final MongoClient mongoClient, final List<SinkRecord> batch) {
        processWriteModels(context, config, mongoClient, buildWriteModels(config, batch));
    }

    List<? extends WriteModel<BsonDocument>> buildWriteModels(final MongoSinkTopicConfig config, final Collection<SinkRecord> records) {
        List<WriteModel<BsonDocument>> docsToWrite = new ArrayList<>(records.size());
        LOGGER.debug("building write model for {} record(s)", records.size());

        PostProcessors postProcessors = config.getPostProcessors();
        records.forEach(record -> {
                    SinkDocument doc = SINK_CONVERTER.convert(record);
                    postProcessors.getPostProcessorList().forEach(pp -> pp.process(doc, record));

                    if (doc.getValueDoc().isPresent()) {
                        docsToWrite.add(config.getWriteModelStrategy().createWriteModel(doc));
                    } else {
                        Optional<WriteModelStrategy> deleteOneModelWriteStrategy = config.getDeleteOneWriteModelStrategy();
                        if (doc.getKeyDoc().isPresent() && deleteOneModelWriteStrategy.isPresent()) {
                            docsToWrite.add(deleteOneModelWriteStrategy.get().createWriteModel(doc));
                        } else {
                            LOGGER.error("skipping sink record {} for which neither key doc nor value doc were present", record);
                        }
                    }
                }
        );
        return docsToWrite;
    }
}
