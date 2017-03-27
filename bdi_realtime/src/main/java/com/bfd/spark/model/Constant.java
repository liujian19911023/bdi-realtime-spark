package com.bfd.spark.model;

public class Constant {
    public static final String KAFKA_SPOUT_NAME = "kafkaSpout";
    public static final String PARSE_ONE_BOLT_NAME = "parseOneBolt";
    public static final String DATA_STAT_BOLT_NAME = "dataStatBolt";
    public static final String PARSE_TWO_BOLT_NAME = "parseTwoBolt";
    public static final String DATA_DETAIL_BOLT_NAME = "dataDetailBolt";

    
    public static final String KAFKA_FIELD_CID = "cid";
    public static final String KAFKA_FIELD_APPKEY = "appkey";
    public static final String KAFKA_FIELD_METHOD = "actionname";
    public static final String KAFKA_FIELD_PAGEFLAG = "pageflag";
    public static final String KAFKA_FIELD_TIMESTAMP = "timestamp";
    public static final String KAFKA_FIELD_GID = "gid";
    public static final String KAFKA_FIELD_TMA = "tma";
    public static final String KAFKA_FIELD_IP = "ip";
    public static final String KAFKA_FIELD_LOCATION = "prov";
    public static final String KAFKA_FIELD_UID = "uid";
    public static final String KAFKA_FIELD_SID = "sid";
    public static final String KAFKA_FIELD_REF_PAGE = "ref_page";
    public static final String KAFKA_FIELD_LN = "ln"; // 前链url
    public static final String KAFKA_FIELD_CURRENT_PAGE = "ep"; // 当前访问页面url
    public static final String KAFKA_FIELD_CURRENT_PAGE_TITLE = "p_s";  // 当前访问页面标题
    public static final String KAFKA_FIELD_OS = "ot";   //操作系统
    public static final String KAFKA_FIELD_RS = "rs"; //分辨率
    public static final String KAFKA_FIELD_BROWSER = "bt"; //浏览器
    public static final String KAFKA_FIELD_COLOR = "cb";
    public static final String KAFKA_FIELD_CODE_TYPE = "ct";
    public static final String KAFKA_FIELD_SUPPORT_JAVA = "ja";
    public static final String KAFKA_FIELD_FLASH_VERSION = "fv";
    public static final String KAFKA_FIELD_LANGUAGE = "oc";
    public static final String KAFKA_FIELD_SUPPORT_COOKIE = "cookiesuport";


    public static final String MONGO_FIELD_APPKEY = "appkey";
    public static final String MONGO_FIELD_DATETIME = "date_time";
    public static final String MONGO_FIELD_PV = "pagevisit_count";
    public static final String MONGO_FIELD_UV = "uservisit_count";
    public static final String MONGO_FIELD_IP_COUNT = "ip_count";
    public static final String MONGO_FIELD_UV_CARD = "uservisit_cardinality";
    public static final String MONGO_FIELD_IP_CARD = "ip_cardinality";
    public static final String MONGO_FIELD_UPDATE_STAMP = "update_stamp";
    public static final String MONGO_FIELD_SID = "sid";
    public static final String MONGO_FIELD_GID = "gid";
    public static final String MONGO_FIELD_LOCATION = "location";
    public static final String MONGO_FIELD_CHANNEL_TYPE = "channel_type";
    public static final String MONGO_FIELD_CHANNEL = "channel";
    public static final String MONGO_FIELD_LANDING_PAGE = "landing_page";
    public static final String MONGO_FIELD_LANDING_PAGE_TITLE = "landing_page_title";
    public static final String MONGO_FIELD_CURRENT_PAGE = "current_page";
    public static final String MONGO_FIELD_CURRENT_PAGE_TITLE = "current_page_title";
    public static final String MONGO_FIELD_IP = "ip";
    public static final String MONGO_FIELD_OS = "os";
    public static final String MONGO_FIELD_RESOLUTION = "resolution";
    public static final String MONGO_FIELD_BROWSER = "browser";
    public static final String MONGO_FIELD_COLOR = "color";
    public static final String MONGO_FIELD_CODE_TYPE = "code_type";
    public static final String MONGO_FIELD_SUPPORT_JAVA = "support_java";
    public static final String MONGO_FIELD_FLASH_VERSION = "flash_version";
    public static final String MONGO_FIELD_LANGUAGE = "language";
    public static final String MONGO_FIELD_PATH = "path";
    public static final String MONGO_FIELD_SUPPORT_COOKIE = "support_cookie";
    public static final String MONGO_FIELD_SESSION_LENGTH = "session_length";
    public static final String MONGO_FIELD_SESSION_PAGES_COUNT = "session_pages_count";

    // 渠道类型
    static final public int BDI_MEDIA_LINK_TYPE_DIRECT = 1;
    static final public int BDI_MEDIA_LINK_TYPE_SEARCH = 2;
    static final public int BDI_MEDIA_LINK_TYPE_EXTERNAL = 3;
    static final public int BDI_MEDIA_LINK_TYPE_INNER = 4;
}
