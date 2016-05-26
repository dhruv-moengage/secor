package com.pinterest.secor.common;

import com.pinterest.secor.message.ParsedMessage;

import java.util.ArrayList;

/**
 * Created by dhruv on 19/05/16.
 */
public class LogFilePathWrap extends LogFilePath {

    public LogFilePathWrap(String prefix, int generation, long lastCommittedOffset, ParsedMessage message, String extension) {
        super(prefix, generation, lastCommittedOffset, message, extension);
    }


    @Override
    public String getLogFileBasename() {
        //Edited final logFileName as date
        String machineName = "defaultMachine";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            machineName = localMachine.getHostName();
        }
        catch(Exception e)
        {

        }
        return "log";
    }
}
