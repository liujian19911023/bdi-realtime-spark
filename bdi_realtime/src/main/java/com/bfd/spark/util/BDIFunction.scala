package com.bfd.spark.util

import org.json.JSONObject

import java.util.Calendar
import java.util.List
import java.util.ArrayList
import java.util.HashSet
import java.util.HashMap

import com.mongodb.BasicDBObject

import org.bson.Document

import org.apache.spark.Accumulator
import java.text.SimpleDateFormat
import java.util.Date
import com.bfd.spark.model.KafkaDetailEndPVJson
import com.bfd.spark.model.DetailPathValue
import com.bfd.spark.model.KafkaDetailPVJson
import com.bfd.spark.model.IPRecord
import com.bfd.spark.model.IpUtil
import com.bfd.spark.model.DetailInfoValue
import com.bfd.spark.model.Global
import com.bfd.spark.model.Constant
import com.bfd.spark.model.LocationInfo






object BDIFunction {
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def extract_pagedoctor(record: String, appkey_patterns: HashMap[String, ArrayList[String]], appkey_urls: HashMap[String, HashSet[String]], has_prefix: Boolean) = {
    var data = record
    if (has_prefix) {
      data = record.substring(record.indexOf("}") + 1)
    }
    val obj = new JSONObject(data)
    val method = obj.getString(Constant.KAFKA_FIELD_METHOD)
    var customer = ""
    if (obj.has("appkey")) {
      customer = obj.getString("appkey")
    } else {
      if (obj.has("cid")) {
        customer = Global.getAppkey(obj.getString("cid"))
      }
    }
    if (method.equals("PageView")) {
      if (obj.has("ep") && obj.has("timestamp")) {
        val ep = obj.getString("ep")
        val uniformUrl = DataUtil.getUniformUrl(customer, ep, appkey_patterns)
        val urls = appkey_urls.get(customer)
        val flag = if (urls == null) false else true
        if (flag && urls.contains(uniformUrl)) {
          val timestamp = obj
            .getDouble("timestamp")
          val times = DataUtil
            .getTime(timestamp)
          val ret = ("1", uniformUrl, customer, "PageView", times.get("year"), "", "", "")
          ret
        } else {
          ("0", "", "", "", "", "", "", "")
        }
      } else {
        ("0", "", "", "", "", "", "", "")
      }
    } else if (method.equals("LinkClick")) {
      if (obj.has("ep") && obj.has("pth")
        && obj.has("timestamp") && obj.has("ln")) {
        val ep = obj.getString("ep")
        val ln = obj.getString("ln")
        val uniformUrl = DataUtil.getUniformUrl(customer, ep, appkey_patterns)
        val urls = appkey_urls.get(customer)
        val flag = if (urls == null) false else true
        if (flag && urls.contains(uniformUrl)) {
          val element = obj.getString("pth")
          val timestamp = obj
            .getDouble("timestamp")
          val times = DataUtil
            .getTime(timestamp)
          val ret = ("2", uniformUrl, customer, "LinkClick", element, times.get("year"), times.get("hour"), ln)

          ret
        } else {
          ("0", "", "", "", "", "", "", "")
        }

      } else {
        ("0", "", "", "", "", "", "", "")
      }

    } else {
      ("0", "", "", "", "", "", "", "")
    }

  }

  def filter_source(record: String, methods: Set[String], has_prefix: Boolean,accu:Accumulator[Long]) = {
    accu += 1
    var data = record
    if (has_prefix) {
      data = record.substring(record.indexOf("}") + 1)
    }
    val obj = new JSONObject(data)
    val method = obj.getString(Constant.KAFKA_FIELD_METHOD)
    //true表示保留
    if (methods.contains(method)) true else false
  }

  def extract_stat(record: String, appkeysList: ArrayList[String], has_prefix: Boolean) = {
    var data = record
    if (has_prefix) {
      data = record.substring(record.indexOf("}") + 1)
    }
    val obj = new JSONObject(data)
    if (obj.has(Constant.KAFKA_FIELD_METHOD)) {
      val method = obj.getString(Constant.KAFKA_FIELD_METHOD)
      if (method.equals("PageView")) {
        var appkey = ""
        if (obj.has("appkey")) {
          appkey = obj.getString("appkey")
        } else if (obj.has("cid")) {
          appkey = Global.getAppkey(obj.getString("cid"))
        }
        val gid = Global.getGid(obj)
        if (appkey != null && appkeysList.contains(appkey)
          && gid != null && obj.has("ip") && obj.has("timestamp")) {
          val ip = obj.getString(Constant.KAFKA_FIELD_IP)
          val timestamp = obj.getDouble(Constant.KAFKA_FIELD_TIMESTAMP)
          val calendar = Calendar.getInstance()
          calendar.setTimeInMillis(Math.round(timestamp * 1.0 * 1000L))
          calendar.set(Calendar.SECOND, 0)
          val date_time = Math.round(Math.floor(calendar.getTimeInMillis() / 1000.0))
          (appkey, gid, ip, date_time)
        } else {
          ("", "", "", "")
        }

      }else{
        ("", "", "", "")
      }
    } else {
      ("", "", "", "")
    }
  }

  def extract_detail(record: String, ipLookup: ArrayList[IPRecord], appkeysList: ArrayList[String], ipMethod: String, has_prefix: Boolean) = {
    var data = record
    if (has_prefix) {
      data = record.substring(record.indexOf("}") + 1)
    }
    val obj = new JSONObject(data)
    if (obj.has(Constant.KAFKA_FIELD_METHOD)) {
      var appkey = ""
      if (obj.has("appkey")) {
        appkey = obj.getString("appkey")
      } else if (obj.has("cid")) {
        appkey = Global.getAppkey(obj.getString("cid"))
      }
      val method = obj.getString(Constant.KAFKA_FIELD_METHOD)
      if (method.equals("PageView")) {
        if (appkey != null && appkeysList.contains(appkey) && obj.has("sid") && obj.has("timestamp")) {
          val timestamp = obj.getDouble(Constant.KAFKA_FIELD_TIMESTAMP)
          val sid = obj.getString(Constant.KAFKA_FIELD_SID)

          val info = new KafkaDetailPVJson()
          info.setAppkey(appkey)
          info.setSid(sid)
          var ip = ""
          if (obj.has(Constant.KAFKA_FIELD_IP)) {
            ip = obj.getString(Constant.KAFKA_FIELD_IP)
          }
          info.setPageflag(if (obj.has(Constant.KAFKA_FIELD_PAGEFLAG)) obj.getString(Constant.KAFKA_FIELD_PAGEFLAG) else null)
          info.setUid(Global.getGid(obj))
          if (ipMethod.equals("item")) {
            info.setLocation(if (obj.has(Constant.KAFKA_FIELD_LOCATION)) obj.getString(Constant.KAFKA_FIELD_LOCATION) else "其他")
          } else {
            var location = ""
            if (ip != null) {
              val ipUtil = new IpUtil()
              val location_info = ipUtil.convertIpToLoc(ip, ipLookup)
              if (location_info != null) {
                location = location_info.asInstanceOf[LocationInfo].locPro
              }
            }
            info.setLocation(if (location == "") "其他" else location)
          }
          info.setIp(ip)
          info.setOs(if (obj.has(Constant.KAFKA_FIELD_OS)) obj.getString(Constant.KAFKA_FIELD_OS) else null)
          info.setResolution(if (obj.has(Constant.KAFKA_FIELD_RS)) obj.getString(Constant.KAFKA_FIELD_RS) else null)
          info.setBrowser(if (obj.has(Constant.KAFKA_FIELD_BROWSER)) obj.getString(Constant.KAFKA_FIELD_BROWSER) else null)
          info.setColor(if (obj.has(Constant.KAFKA_FIELD_COLOR)) obj.getString(Constant.KAFKA_FIELD_COLOR) else null)
          info.setCode_type(if (obj.has(Constant.KAFKA_FIELD_CODE_TYPE)) obj.getString(Constant.KAFKA_FIELD_CODE_TYPE) else null)
          info.setSupport_java(Global.getSupport(obj, Constant.KAFKA_FIELD_SUPPORT_JAVA))
          info.setSupport_cookie(Global.getSupport(obj, Constant.KAFKA_FIELD_SUPPORT_COOKIE))
          info.setFlash_version(if (obj.has(Constant.KAFKA_FIELD_FLASH_VERSION)) obj.getString(Constant.KAFKA_FIELD_FLASH_VERSION) else null)
          info.setLanguage(if (obj.has(Constant.KAFKA_FIELD_LANGUAGE)) obj.getString(Constant.KAFKA_FIELD_LANGUAGE) else null)
          info.setTimestamp(Math.round(timestamp))
          info.setLn_page(if (obj.has(Constant.KAFKA_FIELD_LN)) obj.getString(Constant.KAFKA_FIELD_LN) else null)
          info.setCurrent_page(if (obj.has(Constant.KAFKA_FIELD_CURRENT_PAGE)) obj.getString(Constant.KAFKA_FIELD_CURRENT_PAGE) else null)
          info.setCurrent_page_title(if (obj.has(Constant.KAFKA_FIELD_CURRENT_PAGE_TITLE))
            obj.getString(Constant.KAFKA_FIELD_CURRENT_PAGE_TITLE) else null)
          ("1", appkey, sid, info)

        } else {
          ("0", "", "", None)
        }
      } else if (method.equals("EndPageView")) {
        if (appkey != null && appkeysList.contains(appkey)
          && obj.has("sid") && obj.has("timestamp")
          && obj.has(Constant.KAFKA_FIELD_PAGEFLAG)) {
          val timestamp = obj.getDouble(Constant.KAFKA_FIELD_TIMESTAMP)
          val sid = obj.getString(Constant.KAFKA_FIELD_SID)

          val endInfo = new KafkaDetailEndPVJson()
          endInfo.setAppkey(appkey)
          endInfo.setSid(sid)
          endInfo.setPageflag(obj.getString(Constant.KAFKA_FIELD_PAGEFLAG))
          endInfo.setTimestamp(Math.round(timestamp))
          ("2", appkey, sid, endInfo)
        } else {
          ("0", "", "", None)
        }
      } else {
        ("0", "", "", None)
      }
    } else {
      ("0", "", "", None)
    }
  }
  def string2Long(datestr:String)={
 		val date = DATE_FORMAT.parse(datestr)
 		date.getTime
  }
  def updateDetail(history: Document, type_datas: Iterable[(String, Object)]) = {
    import scala.collection.JavaConversions._
    var detailInfoValue: DetailInfoValue = null
    var endPageView: KafkaDetailEndPVJson = null
    if (history == null) {
      for (type_data <- type_datas) {
        val t = type_data._1
        //PageView
        if (t.equals("1")) {
          val pageView = type_data._2.asInstanceOf[KafkaDetailPVJson]
          if (detailInfoValue == null) {
            detailInfoValue = new DetailInfoValue(pageView)
            val path = new ArrayList[DetailPathValue]()
            path.add(new DetailPathValue(pageView))
            detailInfoValue.setPath(path)
          } else {
            if (pageView.getTimestamp() > detailInfoValue.getMaxTimestamp()) {
              detailInfoValue.setMaxTimestamp(pageView.getTimestamp())
              detailInfoValue.setCurrent_page(pageView.getCurrent_page())
              detailInfoValue.setCurrent_page_title(pageView.getCurrent_page_title())
            } else if (pageView.getTimestamp() < detailInfoValue.getMinTimestamp()) {
              detailInfoValue.setMinTimestamp(pageView.getTimestamp())
              detailInfoValue.setLanding_page(pageView.getCurrent_page())
              detailInfoValue.setLanding_page_title(pageView.getCurrent_page_title())
              detailInfoValue.setLn_page(pageView.getLn_page())
            }
            val path = detailInfoValue.getPath()
            path.add(new DetailPathValue(pageView))
            detailInfoValue.setPath(path)
            detailInfoValue.addCount()
            detailInfoValue.update()
          }
          //EndPageView  
        } else {
          endPageView = type_data._2.asInstanceOf[KafkaDetailEndPVJson]
        }
      }
    } else {
      detailInfoValue = new DetailInfoValue()
      detailInfoValue.setAppkey(history.getString(Constant.MONGO_FIELD_APPKEY))
      detailInfoValue.setSid(history.getString(Constant.MONGO_FIELD_SID))
      detailInfoValue.setLocation(history.getString(Constant.MONGO_FIELD_LOCATION))
      detailInfoValue.setUid(history.getString(Constant.MONGO_FIELD_GID))
      detailInfoValue.setChannel_type(history.getInteger(Constant.MONGO_FIELD_CHANNEL_TYPE))
      
      detailInfoValue.setChannel(history.getString(Constant.MONGO_FIELD_CHANNEL))
      detailInfoValue.setLanding_page(history.getString(Constant.MONGO_FIELD_LANDING_PAGE))
      detailInfoValue.setLanding_page_title(history.getString(Constant.MONGO_FIELD_LANDING_PAGE_TITLE))
      
      detailInfoValue.setCurrent_page(history.getString(Constant.MONGO_FIELD_CURRENT_PAGE))
      detailInfoValue.setCurrent_page_title(history.getString(Constant.MONGO_FIELD_CURRENT_PAGE_TITLE))
      detailInfoValue.setIp(history.getString(Constant.MONGO_FIELD_IP))
      detailInfoValue.setOs(history.getString(Constant.MONGO_FIELD_OS))
      
      detailInfoValue.setResolution(history.getString(Constant.MONGO_FIELD_RESOLUTION))
      detailInfoValue.setBrowser(history.getString(Constant.MONGO_FIELD_BROWSER))
      detailInfoValue.setColor(history.getString(Constant.MONGO_FIELD_COLOR))
      detailInfoValue.setCode_type(history.getString(Constant.MONGO_FIELD_CODE_TYPE))
      detailInfoValue.setSupport_java(history.getBoolean(Constant.MONGO_FIELD_SUPPORT_JAVA))
      detailInfoValue.setSupport_cookie(history.getBoolean(Constant.MONGO_FIELD_SUPPORT_COOKIE))
      
      detailInfoValue.setFlash_version(history.getString(Constant.MONGO_FIELD_FLASH_VERSION))
      detailInfoValue.setLanguage(history.getString(Constant.MONGO_FIELD_LANGUAGE))
      detailInfoValue.setMinTimestamp(history.getLong(Constant.MONGO_FIELD_DATETIME))
      detailInfoValue.setMaxTimestamp(string2Long(history.getString(Constant.MONGO_FIELD_UPDATE_STAMP))/1000)
      val his_paths = history.get(Constant.MONGO_FIELD_PATH).asInstanceOf[List[Document]]
      val path = new ArrayList[DetailPathValue]()
      for (p <- his_paths) {
        val tmp = new DetailPathValue()
        tmp.setUrl(p.getString("url"))
        tmp.setTitle(p.getString("title"))
        tmp.setTimestamp(p.getLong("timestamp"))
        tmp.setVisit_length(p.getLong("visit_length"))
        tmp.setPage_flag(p.getString("page_flag"))
        path.add(tmp)
      }
      detailInfoValue.setPath(path)
      detailInfoValue.setSession_length(history.getInteger(Constant.MONGO_FIELD_SESSION_LENGTH))
      detailInfoValue.setSession_pages_count(history.getInteger(Constant.MONGO_FIELD_SESSION_PAGES_COUNT))
      for (type_data <- type_datas) {
        val t = type_data._1
        //PageView
        if (t.equals("1")) {
          val pageView = type_data._2.asInstanceOf[KafkaDetailPVJson]
          if (pageView.getTimestamp() > detailInfoValue.getMaxTimestamp()) {
            detailInfoValue.setMaxTimestamp(pageView.getTimestamp())
            detailInfoValue.setCurrent_page(pageView.getCurrent_page())
            detailInfoValue.setCurrent_page_title(pageView.getCurrent_page_title())
          } else if (pageView.getTimestamp() < detailInfoValue.getMinTimestamp()) {
            detailInfoValue.setMinTimestamp(pageView.getTimestamp())
            detailInfoValue.setLanding_page(pageView.getCurrent_page())
            detailInfoValue.setLanding_page_title(pageView.getCurrent_page_title())
            detailInfoValue.setLn_page(pageView.getLn_page())
          }
          val path = detailInfoValue.getPath()
          path.add(new DetailPathValue(pageView))
          detailInfoValue.setPath(path)
          detailInfoValue.addCount()
          detailInfoValue.update()
          //EndPageView  
        } else {
          endPageView = type_data._2.asInstanceOf[KafkaDetailEndPVJson]
        }
      }
    }
    val paths = detailInfoValue.getPath
    def comp(path1: DetailPathValue, path2: DetailPathValue) = (path1.getTimestamp < path2.getTimestamp)
    val copy=new ArrayList(paths.sortWith(comp).toList)
    for (i <- 0 until copy.length - 1) {
      copy(i).setVisit_length(copy(i + 1).getTimestamp - copy(i).getTimestamp)
    }
    if (endPageView != null) {
      copy(copy.length - 1).setVisit_length(endPageView.getTimestamp - copy(copy.length - 1).getTimestamp)
    }
    detailInfoValue.setPath(copy)
    detailInfoValue
  }
}