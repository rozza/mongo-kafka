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
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.kafka.connect.Versions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;

public class MongoSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
    private MongoSinkConfig sinkConfig;
    private MongoClient mongoClient;


    @Override
    public String version() {
        return Versions.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("Starting MongoDB sink task");
        try {
            sinkConfig = new MongoSinkConfig(props);
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task", e);
        }
        LOGGER.debug("Started MongoDB sink task");
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.debug("No sink records to process for current poll operation");
            return;
        }
        Map<String, RecordBatches> batchMapping = createSinkRecordBatchesPerTopic(records);
        batchMapping.forEach((topic, batches) -> {
            MongoSinkTopicConfig topicConfig = sinkConfig.getMongoSinkTopicConfig(topic);
            batches.getBufferedBatches().forEach(batch -> {
                        topicConfig.getMongoDataHandler().processSinkRecords(context, topicConfig, getMongoClient(), batch);
                        RateLimitSettings rls = topicConfig.getRateLimitSettings();
                        if (rls.isTriggered()) {
                            LOGGER.debug("Rate limit settings triggering {}ms defer timeout after processing {}"
                                    + " further batches for topic {}", rls.getTimeoutMs(), rls.getEveryN(), topic);
                            try {
                                Thread.sleep(rls.getTimeoutMs());
                            } catch (InterruptedException e) {
                                LOGGER.error(e.getMessage());
                            }
                        }
                    }
            );
        });
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the
     *                       {@link SinkRecord}s passed to {@link #put}.
     */
    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        //NOTE: flush is not used for now...
        LOGGER.debug("Flush called - noop");
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        LOGGER.info("Stopping MongoDB sink task");
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    Map<String, RecordBatches> createSinkRecordBatchesPerTopic(final Collection<SinkRecord> records) {
        LOGGER.debug("Number of sink records to process: {}", records.size());

        Map<String, RecordBatches> batchMapping = new HashMap<>();
        LOGGER.debug("Buffering sink records into grouped topic batches");
        records.forEach(r -> {
            RecordBatches batches = batchMapping.get(r.topic());
            if (batches == null) {
                int maxBatchSize = sinkConfig.getMongoSinkTopicConfig(r.topic()).getInt(MAX_BATCH_SIZE_CONFIG);
                LOGGER.debug("Batch size for collection {} is at most {} record(s)",
                        sinkConfig.getMongoSinkTopicConfig(r.topic()).getNamespace().getCollectionName(), maxBatchSize);
                batches = new RecordBatches(maxBatchSize, records.size());
                batchMapping.put(r.topic(), batches);
            }
            batches.buffer(r);
        });
        return batchMapping;
    }

    private MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(sinkConfig.getConnectionString(), getMongoDriverInformation());
        }
        return mongoClient;
    }
}
