package com.bfd.spark.model;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

public class IPLookup {
    public static ArrayList<IPRecord> getIpMapping(String fileName) throws FileNotFoundException {
        ArrayList<IPRecord> IP_INT_ARRAY = new ArrayList<IPRecord>();
        InputStream input = IPLookup.class.getResourceAsStream(fileName);
        BufferedReader in = new BufferedReader(new InputStreamReader(input));
        String line = null;
        try {
            line = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (line != null) {
            // 取出当前行
            line = StringUtils.trimToEmpty(line);
            if (!StringUtils.isEmpty(line)) {
            	IPRecord record = new IPRecord();
                if (record.updateIPRecord(line)){
                    IP_INT_ARRAY.add(record);
                }
            }

            try {
                line = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return IP_INT_ARRAY;
    }
    

}
