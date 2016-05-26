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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

// TODO(pawel): should we offer a multi-message parser capable of parsing multiple types of
// messages?  E.g., it could be implemented as a composite trying out different parsers and using
// the one that works.  What is the performance cost of such approach?

/**
 * Message parser extracts partitions from messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public abstract class MessageParser {
    protected SecorConfig mConfig;
    protected String[] mNestedFields;
    private static final Logger LOG = LoggerFactory.getLogger(MessageParser.class);

    public MessageParser(SecorConfig config) {
        mConfig = config;
        if (mConfig.getMessageTimestampName() != null &&
            !mConfig.getMessageTimestampName().isEmpty() &&
            mConfig.getMessageTimestampNameSeparator() != null &&
            !mConfig.getMessageTimestampNameSeparator().isEmpty()) {
            String separatorPattern = Pattern.quote(mConfig.getMessageTimestampNameSeparator());
            mNestedFields = mConfig.getMessageTimestampName().split(separatorPattern);
        }
    }

    public ParsedMessage parse(Message message) throws Exception {
        String[] partitions = extractPartitions(message);
        message = transformedMessage(message);
        return new ParsedMessage(message.getTopic(), message.getKafkaPartition(),
                                 message.getOffset(), message.getKafkaKey(),
                                 message.getPayload(), partitions);
    }

    public Message transformedMessage(Message message)
    {
        String inputString = new String(message.getPayload());
        Message returnMessage = message;
        try{
            org.json.JSONObject output = new org.json.JSONObject(inputString);

            String machineName = "defaultMachine";
            try {
                java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
                machineName = localMachine.getHostName();
            }
            catch(Exception e)
            {

            }

            //List[] list = listFromJsonSorted(output.getJSONObject("a"));
            String csv = "";

            int i =0;
//            while(i<list[1].size()) {
//                if (csv.equals(""))
//                    csv += list[1].get(i);
//                else
//                    csv += "," + list[1].get(i);
//
//                if (title.equals(""))
//                    title += list[0].get(i);
//                else
//                    title += "," + list[0].get(i);
//
//
//                i++;
//            }


            String unique_id = output.getString("unique_id");
            String sdk = output.getString("sdk");
            String dBname = output.getString("DBname");
            String event = output.getString("e");
            String user_id = output.getString("user_id");
            String attributes = output.getString("a");
            long timestamp = output.getLong("t");


            String value = user_id+","+sdk;
            String key = machineName+"-"+String.valueOf(System.currentTimeMillis());

            csv+=String.valueOf(timestamp)+","+unique_id+","+attributes+","+user_id+","+sdk;




            returnMessage = new Message(message.getTopic(), message.getKafkaPartition(),
                    message.getOffset(), message.getKafkaKey(), csv.getBytes(Charset.forName("UTF-8")));

            File file1 = FileUtils.getFile("~/Documents/keyMappings.txt");
            FileUtils.write(file1,key+"="+value+"\n",true);
            //File file2 = FileUtils.getFile("~/Documents/eventAttributes.txt");
            //FileUtils.write(file2,"dbName="+dBname+"_"+"event="+event+"-->"+title);



        }
        catch(Exception e)
        {
            System.out.print("Error"+e.getMessage());
        }
        return returnMessage;
    }

    public static List[] listFromJsonSorted(org.json.JSONObject json) {
        if (json == null) return null;
        SortedMap map = new TreeMap();
        Iterator i = json.keys();
        while (i.hasNext()) {
            try {
                String key = i.next().toString();
                //System.out.println(key);
                String j = json.getString(key);
                map.put(key, j);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        LinkedList keys = new LinkedList(map.keySet());
        LinkedList values = new LinkedList(map.values());
        List[] variable= new List[2];
        variable[0] = keys;
        variable[1] = values;
        return variable;
    }

    public abstract String[] extractPartitions(Message payload) throws Exception;
    
    public Object getJsonFieldValue(JSONObject jsonObject) {
        Object fieldValue = null;
        if (mNestedFields != null) {
            Object finalValue = null;            
            for (int i=0; i < mNestedFields.length; i++) {
                if (!jsonObject.containsKey(mNestedFields[i])) {
                    LOG.warn("Could not find key {} in message", mConfig.getMessageTimestampName());
                    break;
                }
                if (i < (mNestedFields.length -1)) {
                    jsonObject = (JSONObject) jsonObject.get(mNestedFields[i]);
                } else {
                    finalValue = jsonObject.get(mNestedFields[i]);
                }
            }
            fieldValue = finalValue;
        } else {
            fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
        }
        return fieldValue;
    }
}
