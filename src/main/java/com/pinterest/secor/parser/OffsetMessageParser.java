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
package com.pinterest.secor.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.sun.tools.classfile.Code_attribute;
import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;

import java.io.File;
import java.nio.charset.Charset;

/**
 * Offset message parser groups messages based on the offset ranges.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class OffsetMessageParser extends MessageParser {
    public OffsetMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        long offset = message.getOffset();
        long offsetsPerPartition = mConfig.getOffsetsPerPartition();
        long partition = (offset / offsetsPerPartition) * offsetsPerPartition;
        String payload = new String(message.getPayload());
        System.out.print("MessageParser called!! -- OffsetMessageParser");
        JsonObject obj = new JsonParser().parse(payload).getAsJsonObject();

        String dbName = obj.get("DBname").getAsString();
        String event = obj.get("e").getAsString();
        dbName = dbName.replaceAll(" ","-");
        event = event.replaceAll(" ","-");
        String timeStamp = new java.text.SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date());

        String[] result = {"dbName=" + dbName,"event="+event,timeStamp};
        return result;
    }


}
