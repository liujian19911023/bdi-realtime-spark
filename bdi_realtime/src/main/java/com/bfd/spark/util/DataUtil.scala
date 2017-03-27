package com.bfd.spark.util

import java.lang.Long
import java.security.MessageDigest
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Calendar
import java.util.Date
import java.util.regex.Pattern
import java.util.HashMap


object DataUtil {
  def getUniformUrl(appkey: String, url: String, appkey_patterns: HashMap[String, ArrayList[String]]) = {
    import scala.collection.JavaConversions._
    val patterns= appkey_patterns.get(appkey)
    var ret=url
    if (patterns != null) {
      for (pattern <- patterns) {
        try {
          val p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE)
          val matcher = p.matcher(url)
          if (matcher.find()) {
            var res = ""
            val count = matcher.groupCount()
            for (i <- 1 to count) {
              res = res + matcher.group(i)
            }
            ret=res
          }
        } catch {
          case _: Throwable =>
        }
      }
    }
    ret
  }

  def getTime(timestamp: Double) = {
    val year = new SimpleDateFormat("yyyy-MM-dd")
    val hour = new SimpleDateFormat("HH:mm")
    val date = new Date(Long.valueOf((timestamp * 1000l).toLong))
    val cal = Calendar.getInstance()
    cal.setTime(date)
    val minute = cal.get(Calendar.MINUTE)
    var td = 0
    if (minute < 10) {
      td = 10 - minute
    } else if (minute < 20) {
      td = 20 - minute
    } else if (minute < 30) {
      td = 30 - minute
    } else if (minute < 40) {
      td = 40 - minute
    } else if (minute < 50) {
      td = 50 - minute
    } else {
      td = 60 - minute
    }
    cal.add(Calendar.MINUTE, td)
    val res =new HashMap[String, String]()
    res.put("year", year.format(cal.getTime()))
    res.put("hour", hour.format(cal.getTime()))
    res
  }

  def string2MD5(inStr: String) = {
    var md5: MessageDigest = null;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch {
      case _: Throwable =>
    }
    val charArray = inStr.toCharArray()
    val byteArray = new Array[Byte](charArray.length)
    for (i <- 0 until charArray.length) {
      byteArray(i) = charArray(i).asInstanceOf[Byte]
    }

    val md5Bytes = md5.digest(byteArray)
    val hexValue = new StringBuffer()
    for (i <- 0 until md5Bytes.length) {
      val tmp = (md5Bytes(i).toInt) & 0xff
      if (tmp < 16)
        hexValue.append("0");
      hexValue.append(Integer.toHexString(tmp));
    }
    hexValue.toString().substring(0, 2);
  }

  def string2timeStamp(date: String, format: String) = {
    try {
      val sdf = new SimpleDateFormat(format)
      val dat = sdf.parse(date)
      dat.getTime() / 1000
    } catch {
      case e: ParseException => {
        System.currentTimeMillis() / 1000
      }
    }
  }
  
}