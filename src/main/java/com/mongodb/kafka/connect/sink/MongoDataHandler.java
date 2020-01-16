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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.List;

public interface MongoDataHandler {

    /**
     * Process the Sink records
     *
     * @param context the context
     * @param config the sink config for the topic
     * @param mongoClient the mongoclient
     * @param batch the batch of records
     */
    void processSinkRecords(final SinkTaskContext context, final MongoSinkTopicConfig config,
                            final MongoClient mongoClient, final List<SinkRecord> batch);
}
