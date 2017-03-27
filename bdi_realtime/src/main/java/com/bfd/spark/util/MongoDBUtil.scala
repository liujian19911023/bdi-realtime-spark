package com.bfd.spark.util

import java.util.ArrayList

import com.mongodb.MongoClient

import org.bson.Document

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection

import java.util.Date
import java.text.SimpleDateFormat

import com.mongodb.client.model.UpdateOptions
import java.util.HashMap
import java.util.HashSet
import com.bfd.spark.model.DetailInfoValue
import com.bfd.spark.model.Constant

object MongoDBUtil {
val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def getAppkeys(db: String, table: String, mongoClient: MongoClient) = {
    import scala.collection.JavaConversions._
    val collection = mongoClient.getDatabase(db).getCollection(table)
    val rets = collection.find()
    val appkeys = new ArrayList[String]()
    for (ret <- rets) {
      val appkey = ret.getString("appkey")
      if (!appkeys.contains(appkey)) {
        appkeys.add(appkey)
      }
    }
    appkeys
  }

  def getSearchEngine(db: String, table: String, mongoClient: MongoClient) ={
    import scala.collection.JavaConversions._
    val collection = mongoClient.getDatabase(db).getCollection(table)
    val rets = collection.find()
    val searchEngine = new HashMap[String, String]()
    for (doc <- rets) {
      val url = doc.getString("engine_url");
      val name = doc.getString("engine_name");
      searchEngine.put(url, name);
    }
    searchEngine
  }
  def findStatByAppkeyAndTime(db: String, table: String, mongoClient: MongoClient, appkey: String, time: Long) = {
    val collection = mongoClient.getDatabase(db).getCollection(table)
    val history = collection.find(new Document().append("appkey", appkey).append("date_time", time)).iterator()
    if (history.hasNext()) {
      (true, history.next(), collection)
    } else {
      (false, new Document(), collection)

    }
  }

  def getAppkeyPattern(db: String, table: String, mongoClient: MongoClient) = {
    import scala.collection.JavaConversions._
    val appkey_patterns = new HashMap[String, ArrayList[String]]()
    val collection = mongoClient.getDatabase(db).getCollection(table)
    val condi = new Document().append("appkey", 1).append("priority", 1)
    val rets = collection.find().sort(condi)
    for (ret <- rets) {
      val appkey = ret.getString("appkey")
      val reg = ret.getString("reg_exp")
      //long priority = ret.getLong("priority")
      var st = appkey_patterns.get(appkey)
      if (null == st) {
        st = new ArrayList[String]()
        appkey_patterns.put(appkey, st)
      }
      st.add(reg)
    }
    appkey_patterns
  }

  def getAppkeyUrl(db: String, table: String, mongoClient: MongoClient, appkey_patterns: HashMap[String, ArrayList[String]]) = {
    import scala.collection.JavaConversions._
    val appkey_urls = new HashMap[String, HashSet[String]]()
    val collection = mongoClient.getDatabase(db).getCollection(table)
    val findCond = new Document().append("is_valid", true)
    val rets = collection.find().filter(findCond)
    for (ret <- rets) {
      val appkey = ret.getString("appkey")
      var url = ret.getString("url")
      url = DataUtil.getUniformUrl(appkey, url, appkey_patterns)
      var urls = appkey_urls.get(appkey)
      if (urls == null) {
        urls = new HashSet[String]()
        appkey_urls.put(appkey, urls)
      }
      urls.add(url)
    }
    appkey_urls
  }

  def updateDetail(value: DetailInfoValue, collection: MongoCollection[Document]) {

    import scala.collection.JavaConversions._
    
    val paths = value.getPath()
    val alist = new ArrayList[Document]()
    for (path <- paths) {
      val a = new Document().append("page_flag", path.getPage_flag())
        .append("url", path.getUrl())
        .append("title", path.getTitle())
        .append("timestamp", path.getTimestamp())
        .append("visit_length", path.getVisit_length())
      alist.add(a)
    }
    val updateCondition = new Document()
      .append(Constant.MONGO_FIELD_APPKEY, value.getAppkey())
      .append(Constant.MONGO_FIELD_SID, value.getSid())
    val updateValue = new Document()
      .append(Constant.MONGO_FIELD_DATETIME, value.getMinTimestamp())
      .append(Constant.MONGO_FIELD_GID, value.getUid())
      .append(Constant.MONGO_FIELD_LOCATION, value.getLocation())
      .append(Constant.MONGO_FIELD_CHANNEL_TYPE, value.getChannel_type())
      .append(Constant.MONGO_FIELD_CHANNEL, value.getChannel())
      .append(Constant.MONGO_FIELD_LANDING_PAGE, value.getLanding_page())
      .append(Constant.MONGO_FIELD_LANDING_PAGE_TITLE, value.getLanding_page_title())
      .append(Constant.MONGO_FIELD_CURRENT_PAGE, value.getCurrent_page())
      .append(Constant.MONGO_FIELD_CURRENT_PAGE_TITLE, value.getCurrent_page_title())
      .append(Constant.MONGO_FIELD_IP, value.getIp())
      .append(Constant.MONGO_FIELD_OS, value.getOs())
      .append(Constant.MONGO_FIELD_RESOLUTION, value.getResolution())
      .append(Constant.MONGO_FIELD_BROWSER, value.getBrowser())
      .append(Constant.MONGO_FIELD_COLOR, value.getColor())
      .append(Constant.MONGO_FIELD_CODE_TYPE, value.getCode_type())
      .append(Constant.MONGO_FIELD_SUPPORT_JAVA, value.getSupport_java())
      .append(Constant.MONGO_FIELD_SUPPORT_COOKIE, value.getSupport_cookie())
      .append(Constant.MONGO_FIELD_FLASH_VERSION, value.getFlash_version())
      .append(Constant.MONGO_FIELD_LANGUAGE, value.getLanguage())
      .append(Constant.MONGO_FIELD_PATH, alist)
      .append(Constant.MONGO_FIELD_SESSION_LENGTH, value.getSession_length())
      .append(Constant.MONGO_FIELD_SESSION_PAGES_COUNT, value.getSession_pages_count())
      .append(Constant.MONGO_FIELD_UPDATE_STAMP, DATE_FORMAT.format(new Date(value.getMaxTimestamp()*1000)))
    collection.updateOne(updateCondition,
      new Document("$set", updateValue),
      new UpdateOptions().upsert(true))

  }

}