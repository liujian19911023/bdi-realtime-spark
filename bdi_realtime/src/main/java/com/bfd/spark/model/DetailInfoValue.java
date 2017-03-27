package com.bfd.spark.model;

import java.io.Serializable;
import java.util.ArrayList;

public class DetailInfoValue implements Serializable {
    private String appkey;
    private String sid;
    private Long maxTimestamp;
    private Long minTimestamp;
    private String uid;
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
    private Integer channel_type;
    private String channel;
    private String ln_page; // 入口页面的前链
    private String landing_page;  // 入口页面url
    private String landing_page_title; // 入口页面标题
    private String current_page; // 当前访问页面url
    private String current_page_title; // 当前访问页面标题
    private ArrayList<DetailPathValue> path;
    private Integer session_length;
    private Integer session_pages_count;
    private boolean isChange;

    public DetailInfoValue() {
    }

    public DetailInfoValue(KafkaDetailPVJson kafkaKey) {
        this.setAppkey(kafkaKey.getAppkey());
        this.setSid(kafkaKey.getSid());
        this.setMinTimestamp(kafkaKey.getTimestamp());
        this.setMaxTimestamp(kafkaKey.getTimestamp());
        this.setUid(kafkaKey.getUid());
        this.setLocation(kafkaKey.getLocation());
        this.setIp(kafkaKey.getIp());
        this.setOs(kafkaKey.getOs());
        this.setResolution(kafkaKey.getResolution());
        this.setBrowser(kafkaKey.getBrowser());
        this.setColor(kafkaKey.getColor());
        this.setCode_type(kafkaKey.getCode_type());
        this.setSupport_java(kafkaKey.getSupport_java());
        this.setSupport_cookie(kafkaKey.getSupport_cookie());
        this.setFlash_version(kafkaKey.getFlash_version());
        this.setLanguage(kafkaKey.getLanguage());
        this.setLn_page(kafkaKey.getLn_page());
        this.setLanding_page(kafkaKey.getCurrent_page());
        this.setLanding_page_title(kafkaKey.getCurrent_page_title());
        this.setCurrent_page(kafkaKey.getCurrent_page());
        this.setCurrent_page_title(kafkaKey.getCurrent_page_title());
        this.setIsChange(true);
        this.setSession_length(0);
        this.setSession_pages_count(1);

    }

    public void update() {
        this.session_length = new Long(maxTimestamp - minTimestamp).intValue();
        this.isChange = true;
    }

    public void addCount() {
        this.session_pages_count += 1;
    }


    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(Long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public Long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(Long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
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

    public Integer getChannel_type() {
        return channel_type;
    }

    public void setChannel_type(Integer channel_type) {
        this.channel_type = channel_type;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getLn_page() {
        return ln_page;
    }

    public void setLn_page(String ln_page) {
        this.ln_page = ln_page;
    }

    public String getLanding_page() {
        return landing_page;
    }

    public void setLanding_page(String landing_page) {
        this.landing_page = landing_page;
    }

    public String getLanding_page_title() {
        return landing_page_title;
    }

    public void setLanding_page_title(String landing_page_title) {
        this.landing_page_title = landing_page_title;
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

    public ArrayList<DetailPathValue> getPath() {
        return path;
    }

    public void setPath(ArrayList<DetailPathValue> path) {
        this.path = path;
    }

    public Integer getSession_length() {
        return session_length;
    }

    public void setSession_length(Integer session_length) {
        this.session_length = session_length;
    }

    public Integer getSession_pages_count() {
        return session_pages_count;
    }

    public void setSession_pages_count(Integer session_pages_count) {
        this.session_pages_count = session_pages_count;
    }

    public boolean isChange() {
        return isChange;
    }

    public void setIsChange(boolean isChange) {
        this.isChange = isChange;
    }

    @Override
    public String toString() {
        return "DetailInfoValue{" +
                "appkey='" + appkey + '\'' +
                ", sid='" + sid + '\'' +
                ", maxTimestamp=" + maxTimestamp +
                ", minTimestamp=" + minTimestamp +
                ", uid='" + uid + '\'' +
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
                ", channel_type=" + channel_type +
                ", channel='" + channel + '\'' +
                ", ln_page='" + ln_page + '\'' +
                ", landing_page='" + landing_page + '\'' +
                ", landing_page_title='" + landing_page_title + '\'' +
                ", current_page='" + current_page + '\'' +
                ", current_page_title='" + current_page_title + '\'' +
                ", path=" + path +
                ", session_length=" + session_length +
                ", session_pages_count=" + session_pages_count +
                ", isChange=" + isChange +
                '}';
    }
}

