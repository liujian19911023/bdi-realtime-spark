package com.bfd.spark.model;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.jetty.security.Credential;

public class Global {
    static final private Pattern sessionIDPattern = Pattern.compile("^(\\d+\\.){2}\\d+",Pattern.CASE_INSENSITIVE);

    static public int getFileLen(String fileName) throws FileNotFoundException {
        int count = 0;

        BufferedReader in = new BufferedReader(new FileReader(fileName));

        String line = null;
        try {
            line = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (line != null) {
            try {
                line = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }

        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return count;
    }

    public static String getAppkey(String cid) {
        if (cid == null) {
            return null;
        }
        return Credential.MD5.digest(cid + "_bdi").substring(4);
    }

    public static Long getSidTimestamp(String sid) {
        return Long.parseLong(sid.substring(sid.lastIndexOf(".") + 1)) / 1000l;
    }

    public static String getGid(JSONObject obj) throws JSONException {
        if (!obj.has(Constant.KAFKA_FIELD_GID) && !obj.has(Constant.KAFKA_FIELD_TMA)){
            return null;
        }

        String gid = null;
        if (obj.has(Constant.KAFKA_FIELD_TMA)){
            Matcher matcher = sessionIDPattern.matcher(obj.getString(Constant.KAFKA_FIELD_TMA));
            if (matcher.find()){
                gid = matcher.group();
            }
        }
        if (gid == null && obj.has(Constant.KAFKA_FIELD_GID)){
            gid = obj.getString(Constant.KAFKA_FIELD_GID);
        }

        return gid;
    }

    public static Boolean getSupport(JSONObject obj, String item) {
        if (obj.has(item)) {
            try {
                Object val = obj.get(item);
                if (val.getClass() == String.class) {
                    String value = (String) val;
                    if (value.equals("true") || value.equals("1")) {
                        return true;
                    }
                } else if (val.getClass() == Integer.class) {
                    Integer value = (Integer) val;
                    if (value.equals(1)) {
                        return true;
                    }
                } else if (val.getClass() == Boolean.class) {
                    Boolean value = (Boolean) val;
                    return value;
                }
            } catch (JSONException e) {
                return false;
            }
        }

        return false;
    }
}
