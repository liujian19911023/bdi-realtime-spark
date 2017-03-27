package com.bfd.spark.model;

import java.io.Serializable;

public class KafkaDetailEndPVJson implements Serializable{
    private String appkey;
    private Long timestamp; //
    private String sid;
    private String pageflag;

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getPageflag() {
        return pageflag;
    }

    public void setPageflag(String pageflag) {
        this.pageflag = pageflag;
    }


    @Override
    public String toString() {
        return "KafkaDetailEndPVJson{" +
                "appkey='" + appkey + '\'' +
                ", timestamp=" + timestamp +
                ", sid='" + sid + '\'' +
                ", pageflag='" + pageflag + '\'' +
                '}';
    }
}

