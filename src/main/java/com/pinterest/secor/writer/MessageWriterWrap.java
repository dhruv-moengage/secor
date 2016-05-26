package com.pinterest.secor.writer;

import com.pinterest.secor.common.*;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.message.ParsedMessage;

/**
 * Created by dhruv on 19/05/16.
 */
public class MessageWriterWrap extends MessageWriter {
    public MessageWriterWrap(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry) throws Exception {
        super(config, offsetTracker, fileRegistry);
    }

    @Override
    public void write(ParsedMessage message) throws Exception {
        System.out.println("MessageWriterWrapper called!! - write()");
        TopicPartition topicPartition = new TopicPartition(message.getTopic(),
                message.getKafkaPartition());
        long offset = mOffsetTracker.getAdjustedCommittedOffsetCount(topicPartition);
        LogFilePathWrap path = new LogFilePathWrap(mLocalPrefix, mGeneration, offset, message,
                mFileExtension);
        System.out.println("PATH ------"+path);
        FileWriter writer = mFileRegistry.getOrCreateWriter(path, mCodec);
        writer.write(new KeyValue(message.getOffset(), message.getKafkaKey(), message.getPayload()));

    }
}
