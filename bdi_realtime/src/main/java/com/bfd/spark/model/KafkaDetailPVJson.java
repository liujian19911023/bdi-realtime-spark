package com.bfd.spark.model;

import java.io.Serializable;

public class KafkaDetailPVJson implements Serializable {
    private String appkey;
    private Long timestamp; //
    private String uid;
    private String sid;
    private String pageflag;
    private String location;
    private String ip;
    private String os;
    private String resolution;
    private String browser;
    private String color;
    private String code_type;
    private Boolean support_java;
    private Boolean support_cookie;
    private String flash_version;
    private String language;
    private String ln_page;  // 前链
    private String current_page; // 当前访问页面url
    private String current_page_title; // 当前访问页面标题


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

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getLn_page() {
        return ln_page;
    }

    public void setLn_page(String ln_page) {
        this.ln_page = ln_page;
    }

    public String getCurrent_page() {
        return current_page;
    }

    public void setCurrent_page(String current_page) {
        this.current_page = current_page;
    }

    public String getCurrent_page_title() {
        return current_page_title;
    }

    public void setCurrent_page_title(String current_page_title) {
        this.current_page_title = current_page_title;
    }

    public String getCode_type() {
        return code_type;
    }

    public void setCode_type(String code_type) {
        this.code_type = code_type;
    }

    public Boolean getSupport_java() {
        return support_java;
    }

    public void setSupport_java(Boolean support_java) {
        this.support_java = support_java;
    }

    public Boolean getSupport_cookie() {
        return support_cookie;
    }

    public void setSupport_cookie(Boolean support_cookie) {
        this.support_cookie = support_cookie;
    }

    public String getFlash_version() {
        return flash_version;
    }

    public void setFlash_version(String flash_version) {
        this.flash_version = flash_version;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    @Override
    public String toString() {
        return "KafkaDetailPVJson{" +
                "appkey='" + appkey + '\'' +
                ", timestamp=" + timestamp +
                ", uid='" + uid + '\'' +
                ", sid='" + sid + '\'' +
                ", pageflag='" + pageflag + '\'' +
                ", location='" + location + '\'' +
                ", ip='" + ip + '\'' +
                ", os='" + os + '\'' +
                ", resolution='" + resolution + '\'' +
                ", browser='" + browser + '\'' +
                ", color='" + color + '\'' +
                ", code_type='" + code_type + '\'' +
                ", support_java=" + support_java +
                ", support_cookie=" + support_cookie +
                ", flash_version='" + flash_version + '\'' +
                ", language='" + language + '\'' +
                ", ln_page='" + ln_page + '\'' +
                ", current_page='" + current_page + '\'' +
                ", current_page_title='" + current_page_title + '\'' +
                '}';
    }
}
