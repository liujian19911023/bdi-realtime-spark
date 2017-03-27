package com.bfd.spark.model;

import java.io.Serializable;


public class DetailPathValue implements Serializable {
    private String page_flag;
    private String url;
    private String title;
    private Long timestamp;
    private Long visit_length;

    public DetailPathValue() {
    }

    public DetailPathValue(KafkaDetailPVJson json) {
        this.page_flag = json.getPageflag();
        this.url = json.getCurrent_page();
        this.title = json.getCurrent_page_title();
        this.timestamp = json.getTimestamp();
        this.visit_length = 0l;
    }

    public String getPage_flag() {
        return page_flag;
    }

    public void setPage_flag(String page_flag) {
        this.page_flag = page_flag;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getVisit_length() {
        return visit_length;
    }

    public void setVisit_length(Long visit_length) {
        this.visit_length = visit_length;
    }
    
    

    @Override
    public String toString() {
        return "DetailPathValue{" +
                "page_flag='" + page_flag + '\'' +
                ", url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", timestamp=" + timestamp +
                ", visit_length=" + visit_length +
                '}';
    }

	



}

