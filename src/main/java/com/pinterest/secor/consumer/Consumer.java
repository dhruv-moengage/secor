/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.consumer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.transformer.MessageTransformer;
import com.pinterest.secor.uploader.Uploader;
import com.pinterest.secor.uploader.UploadManager;
import com.pinterest.secor.reader.MessageReader;
import com.pinterest.secor.util.ReflectionUtil;
import com.pinterest.secor.writer.MessageWriter;

import com.pinterest.secor.writer.MessageWriterWrap;
import kafka.consumer.ConsumerTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread;
import java.nio.charset.Charset;
/**
 * Consumer is a top-level component coordinating reading, writing, and uploading Kafka log
 * messages.  It is implemented as a thread with the intent of running multiple consumer
 * concurrently.
 *
 * Note that consumer is not fixed with a specific topic partition.  Kafka rebalancing mechanism
 * allocates topic partitions to consumers dynamically to accommodate consumer population changes.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Consumer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private SecorConfig mConfig;

    private MessageReader mMessageReader;
    private MessageWriter mMessageWriter;
    private MessageParser mMessageParser;
    private OffsetTracker mOffsetTracker;
    private MessageTransformer mMessageTransformer;
    private Uploader mUploader;
    // TODO(pawel): we should keep a count per topic partition.
    private double mUnparsableMessages;

    public Consumer(SecorConfig config) {
        mConfig = config;
    }

    private void init() throws Exception {
        mOffsetTracker = new OffsetTracker();
        mMessageReader = new MessageReader(mConfig, mOffsetTracker);
        FileRegistry fileRegistry = new FileRegistry(mConfig);
        UploadManager uploadManager = ReflectionUtil.createUploadManager(mConfig.getUploadManagerClass(), mConfig);

        mUploader = ReflectionUtil.createUploader(mConfig.getUploaderClass());
        mUploader.init(mConfig, mOffsetTracker, fileRegistry, uploadManager);
        mMessageWriter = new MessageWriter(mConfig, mOffsetTracker, fileRegistry);
        mMessageParser = ReflectionUtil.createMessageParser(mConfig.getMessageParserClass(), mConfig);
        mMessageTransformer =  ReflectionUtil.createMessageTransformer(mConfig.getMessageTransformerClass(), mConfig);
        mUnparsableMessages = 0.;
    }

    @Override
    public void run() {
        try {
            // init() cannot be called in the constructor since it contains logic dependent on the
            // thread id.
            System.out.println("Consumer run() - NEW CONSUMER CREATED!");
            init();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the consumer", e);
        }

        // check upload policy every N seconds or 10,000 messages/consumer timeouts
        long checkEveryNSeconds = Math.min(10 * 60, mConfig.getMaxFileAgeSeconds() / 2);
        long checkMessagesPerSecond = mConfig.getMessagesPerSecond();
        long nMessages = 0;
        long lastChecked = System.currentTimeMillis();
        while (true) {
            boolean hasMoreMessages = consumeNextMessage();
            if (!hasMoreMessages) {
                break;
            }

            long now = System.currentTimeMillis();
            if (nMessages++ % checkMessagesPerSecond == 0 ||
                    (now - lastChecked) > checkEveryNSeconds * 1000) {
                lastChecked = now;
                checkUploadPolicy();
            }
        }
        checkUploadPolicy();
    }

    private void checkUploadPolicy() {
        try {
            mUploader.applyPolicy();
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply upload policy", e);
        }
    }

    // @return whether there are more messages left to consume
    private boolean consumeNextMessage() {
        Message rawMessage = null;
        try {
            boolean hasNext = mMessageReader.hasNext();
            if (!hasNext) {
                return false;
            }
            rawMessage = mMessageReader.read();
        } catch (ConsumerTimeoutException e) {
            // We wait for a new message with a timeout to periodically apply the upload policy
            // even if no messages are delivered.
            LOG.trace("Consumer timed out", e);
        }
        if (rawMessage != null) {
            // Before parsing, update the offset and remove any redundant data
            try {
                mMessageWriter.adjustOffset(rawMessage);
            } catch (IOException e) {
                throw new RuntimeException("Failed to adjust offset.", e);
            }


            //Split the message to subparts - eventParts
            int size = 0;
            try {
                String rawMessageString = new String(rawMessage.getPayload());
                JsonObject obj = new JsonParser().parse(rawMessageString).getAsJsonObject();
                JsonArray eventArray = obj.getAsJsonArray("action");
                size = eventArray.size();


            int i = 0;
            while (i < size) {
                ParsedMessage parsedMessage = null;

                JsonObject subMessage = new JsonObject();
                subMessage.addProperty("e", ((JsonObject) eventArray.get(i)).get("n").getAsString());
                subMessage.addProperty("t", ((JsonObject) eventArray.get(i)).get("t").getAsLong());
                subMessage.add("a", ((JsonObject) eventArray.get(i)).get("a").getAsJsonObject());
                subMessage.addProperty("unique_id", obj.get("unique_id").getAsString());
                subMessage.addProperty("sdk", obj.get("sdk").getAsString());
                subMessage.addProperty("user_id", obj.get("user_id").getAsString());
                subMessage.addProperty("DBname", obj.get("DBname").getAsString());
                String subMessageString = subMessage.toString();
                Message message = new Message(rawMessage.getTopic(), rawMessage.getKafkaPartition(),
                        rawMessage.getOffset(), rawMessage.getKafkaKey(), subMessageString.getBytes(Charset.forName("UTF-8")));

                //Parse eventParts and write to corresponding files

                Message transformedMessage = mMessageTransformer.transform(message);
                parsedMessage = mMessageParser.parse(transformedMessage);
                System.out.println("-----------------------Payload-------"+new String(parsedMessage.getPayload()));
                final double DECAY = 0.999;
                mUnparsableMessages *= DECAY;


                if (parsedMessage != null) {
                    try {
                        mMessageWriter.write(parsedMessage);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to write message " + parsedMessage, e);
                    }
                }
                i++;

            }

        }catch (Throwable e) {
            mUnparsableMessages++;
            final double MAX_UNPARSABLE_MESSAGES = 1000.;
            if (mUnparsableMessages > MAX_UNPARSABLE_MESSAGES) {
                throw new RuntimeException("Failed to parse message " + rawMessage, e);
            }
            LOG.warn("Failed to parse message {}", rawMessage, e);
        }









        }
        return true;
    }

    /**
     * Helper to get the offset tracker (used in tests)
     *
     * @return
     */
    public OffsetTracker getOffsetTracker() {
        return this.mOffsetTracker;
    }
}
