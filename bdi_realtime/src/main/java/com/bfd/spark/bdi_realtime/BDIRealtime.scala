package com.bfd.spark.bdi_realtime

import java.io.FileInputStream
import java.io.PrintWriter
import java.io.StringWriter
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Calendar
import java.util.Date
import java.util.Properties
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.bson.Document

import com.bfd.spark.model.Constant
import com.bfd.spark.model.DetailInfoValue
import com.bfd.spark.model.DetailPathValue
import com.bfd.spark.model.IPLookup
import com.bfd.spark.model.KafkaDetailEndPVJson
import com.bfd.spark.model.KafkaDetailPVJson
import com.bfd.spark.schedule.TimeSchedule
import com.bfd.spark.util.BDIFunction
import com.bfd.spark.util.MongoDBUtil
import com.clearspring.analytics.stream.cardinality.AdaptiveCounting
import com.mongodb.BasicDBObject
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOptions

import consumer.kafka.ProcessedOffsetManager
import consumer.kafka.ReceiverLauncher
import com.bfd.spark.pool.HbasePool
import com.bfd.spark.util.DataUtil
import org.apache.hadoop.hbase.util.Bytes
import com.bfd.spark.pool.MongodbPool
import com.bfd.spark.model.SearchEngine
import org.bson.types.Binary
import org.apache.spark.streaming.dstream.DStream
import consumer.kafka.MessageAndMetadata
import consumer.kafka.NoTopicException

/**
 * BDI realtime statistic.
 * author jian.liu
 */
object BDIRealtime {
  private var ssc: StreamingContext = null
  private var timer: Timer = null
  private var schedule: TimeSchedule = null
  private var thread: Thread = null
  private var confThread: Thread = null
  private val ip_file = "/bfd_ip_city.properties"
  val methods = Set[String]("PageView", "EndPageView", "LinkClick")
  val LOG = Logger.getLogger(BDIRealtime.getClass)
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage spark-submit   --class com.bfd.spark.bdi_realtime.BDIRealtime  --master yarn  --name bdi_realtime  --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=200000  --files log4j.properties,bdi_realtime.properties  --num-executors 10  --driver-memory 10g  --executor-memory 9g  --executor-cores 20 bdi_realtime-1.0.0-jar-with-dependencies.jar  bdi_realtime.properties log4j.properties")
    }
    val prop = new Properties()
    val in = new FileInputStream(args(0))
    prop.load(in)
    in.close()
    PropertyConfigurator.configure(args(1))
    //kafka conf
    val kafka_zk_address = prop.getProperty("kafka.zk.address")
    LOG.info("kafka.zk.address-->" + kafka_zk_address)
    val kafka_group = prop.getProperty("kafka.group")
    LOG.info("kafka.group-->" + kafka_group)
    val iterval_second = prop.getProperty("interval.second").toInt
    LOG.info("interval.second-->" + iterval_second)
    val num_of_thread = prop.getProperty("num.kafka.read.thread").toInt
    LOG.info("num.kafka.read.thread-->" + num_of_thread)
    val hasprefix = prop.getProperty("kafka.data.hasprefix").toBoolean
    LOG.info("kafka.data.hasprefix-->" + hasprefix)
    //switch time
    val hour = prop.getProperty("topic.switch.hour").toInt
    LOG.info("topic.switch.hour-->" + hour)
    val minute = prop.getProperty("topic.switch.minute").toInt
    LOG.info("topic.switch.hour-->" + hour)
    //zk conf for kafka
    val zkhosts = kafka_zk_address.split("/")(0).split(",").map { x => x.split(":")(0) }.mkString(",")
    LOG.info("zkhosts-->" + zkhosts)
    val zkports = kafka_zk_address.split("/")(0).split(",")(0).split(":")(1)
    LOG.info("zkports-->" + zkports)
    val zookeeper_consumer_connection = kafka_zk_address.split("/")(0)
    LOG.info("zookeeper_consumer_connection-->" + zookeeper_consumer_connection)
    val zookeeper_consumer_path = "/" + kafka_group
    LOG.info("zookeeper_consumer_path-->" + zookeeper_consumer_path)
    val tmp = if (kafka_zk_address.split("/").length == 1) "" else kafka_zk_address.split("/")(1)
    val brokerPath = if (tmp.equals("")) "/brokers" else "/" + tmp + "/brokers"
    LOG.info("brokerPath-->" + brokerPath)
    //hbase conf for pagedoctor
    val hbase_rootdir = prop.getProperty("hbase.rootdir")
    LOG.info("hbase.rootdir-->" + hbase_rootdir)
    val hbase_zookeeper_quorum = prop.getProperty("hbase.zookeeper.quorum")
    LOG.info("hbase.zookeeper.quorum-->" + hbase_zookeeper_quorum)
    val hbase_zookeeper_porperty_clientPort = prop.getProperty("hbase.zookeeper.property.clientPort")
    LOG.info("hbase.zookeeper.property.clientPort-->" + hbase_zookeeper_porperty_clientPort)
    val zookeeper_znode_parent = prop.getProperty("zookeeper.znode.parent")
    LOG.info("zookeeper.znode.parent-->" + zookeeper_znode_parent)
    val hbase_table = prop.getProperty("hbase.table")
    LOG.info("hbase.table-->" + hbase_table)
    //mongodb conf for connection
    val mongo_url = prop.getProperty("mongo.uri")
    LOG.info("mongo.uri-->" + mongo_url)
    val mongo_db = prop.getProperty("mongo.db")
    LOG.info("mongo.db-->" + mongo_db)
    //conf table and result table
    val mongo_appkey_table = prop.getProperty("mongodb.appkey.table")
    LOG.info("mongodb.appkey.table-->" + mongo_appkey_table)
    val mongo_reg_table = prop.getProperty("mongodb.reg.table")
    LOG.info("mongodb.reg.table-->" + mongo_reg_table)
    val mongo_doctor_table = prop.getProperty("mongodb.doctor.table")
    LOG.info("mongodb.doctor.table-->" + mongo_doctor_table)
    val mongo_stat_table = prop.getProperty("mongodb.stat.table")
    LOG.info("mongodb.stat.table-->" + mongo_stat_table)
    val mongo_detail_table = prop.getProperty("mongodb.detail.table")
    LOG.info("mongodb.detail.table-->" + mongo_detail_table)
    val mongo_searchengine_table = prop.getProperty("mongodb.searchengine.table")
    LOG.info("mongodb.searchengine.table-->" + mongo_searchengine_table)
    //ip conf  file |item
    val detail_ip_location = prop.getProperty("detial.ip.location.method")
    LOG.info("detial.ip.location.method-->" + detail_ip_location)
    val card_init_bits = prop.getProperty("card.init.bits").toInt
    LOG.info("card.init.bits-->" + card_init_bits)
    val conf_update_interval_minute = prop.getProperty("conf.update.interval.minute").toInt
    //get daily topic
    def getTopic() = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      sdf.format(new Date())
    }
    //get yesterday topic
    def yesterday = {
      val startDT = Calendar.getInstance()
      startDT.setTime(new Date())
      startDT.add(Calendar.DAY_OF_MONTH, -1)
      val yes = startDT.getTime()
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      sdf.format(yes)
    }
    //record whether data avialable
    var changed = true
    //before switch to new topic,release resource
    def stopLastDay() {
      val yes = yesterday
      def extracted() = {
        while (changed) {
          LOG.info("topic " + yes + " still has data")
          TimeUnit.SECONDS.sleep(iterval_second)
        }
        LOG.info("determine once again")
        var need_wait = false
        for (i <- 0 to 3) {
          if (changed && !need_wait) need_wait = true
          TimeUnit.SECONDS.sleep(iterval_second)
          LOG.info("wait")
        }
        need_wait
      }
      var need_loop = extracted()
      while (need_loop) {
        LOG.info("loop")
        need_loop = extracted()
      }
      if (thread != null) {
        LOG.info("stop thread")
        thread.interrupt()
        thread = null
      }

      if (confThread != null) {
        LOG.info("stop conf thread")
        confThread.interrupt()
        confThread = null
      }

      if (ssc != null) {
        LOG.info("stop ssc")
        ssc.stop(true, true)
        ssc = null
      }
      if (timer != null) {
        LOG.info("stop timer")
        timer.cancel()
        timer = null
      }
      if (schedule != null) {
        LOG.info("stop schedule")
        schedule = null
      }
    }

    //start a spark streaming job
    def startStreamContext() = {
      LOG.info("start ssc")
      changed = true
      val sparkConf = new SparkConf()
      ssc = new StreamingContext(sparkConf, Seconds(iterval_second))
      //这个mongodb客户端用于更新配置
      val mongoClient = MongodbPool.getMongoClient(mongo_url)
      val appkeys = MongoDBUtil.getAppkeys(mongo_db, mongo_appkey_table, mongoClient)
      var appkeys_broad = ssc.sparkContext.broadcast(appkeys)
      val ip_Method_broad = ssc.sparkContext.broadcast(detail_ip_location)
      val ipLookUp = IPLookup.getIpMapping(ip_file)
      val ipLookUp_broad = ssc.sparkContext.broadcast(ipLookUp)
      val appkeys_pattern = MongoDBUtil.getAppkeyPattern(mongo_db, mongo_reg_table, mongoClient)
      var appkeys_pattern_broad = ssc.sparkContext.broadcast(appkeys_pattern)
      val appkeys_urls = MongoDBUtil.getAppkeyUrl(mongo_db, mongo_doctor_table, mongoClient, appkeys_pattern)
      var appkeys_urls_broad = ssc.sparkContext.broadcast(appkeys_urls)
      val searchEngine = MongoDBUtil.getSearchEngine(mongo_db, mongo_searchengine_table, mongoClient)
      var searchEngine_broad = ssc.sparkContext.broadcast(searchEngine)
      val accu = ssc.sparkContext.accumulator(0l)
      var pre = 0l
      //get accumulator value every iterval second,when switch topic,we can determine whether daily topic has data
      val topic = getTopic()
      //Specify number of Receivers you need. 
      val numberOfReceivers = num_of_thread
      val kafkaProperties: Map[String, String] =
        Map("zookeeper.hosts" -> zkhosts,
          "zookeeper.port" -> zkports,
          "zookeeper.broker.path" -> brokerPath,
          "kafka.topic" -> topic,
          "zookeeper.consumer.connection" -> zookeeper_consumer_connection,
          "zookeeper.consumer.path" -> zookeeper_consumer_path,
          "kafka.consumer.id" -> kafka_group,
          //optional properties
          "consumer.forcefromstart" -> "true",
          "consumer.backpressure.enabled" -> "true")

      val props = new java.util.Properties()
      kafkaProperties foreach { case (key, value) => props.put(key, value) }
      var tmp_stream: DStream[MessageAndMetadata] = null
      //when new topic not in kafka,we wait it
      while (tmp_stream == null) {
        try {
          tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_AND_DISK)
        } catch {
          case e: NoTopicException =>
            LOG.info(exceptionToString(e))
            LOG.info("wait for topic create for 20 second")
            TimeUnit.SECONDS.sleep(20)
        }
      }
      LOG.info("create accu thread")
      //help to determine whether to stop today topic
      thread = new Thread() {
        override def run() {
          while (!Thread.interrupted()) {
            try {
              if (accu.value - pre == 0) changed = false else changed = true
              pre = accu.value
              LOG.info("monitor thread accu size is " + pre)
              TimeUnit.SECONDS.sleep(iterval_second)
            } catch {
              case e: Throwable => LOG.error(exceptionToString(e))
            }
          }
        }
      }
      thread.start()
      LOG.info("create update conf thread")
      //background thread to update conf
      confThread = new Thread() {

        override def run() {
          while (!Thread.interrupted()) {
            try {
              LOG.info("conf update thread sleep " + conf_update_interval_minute)
              TimeUnit.MINUTES.sleep(10)
              val appkeys_update = MongoDBUtil.getAppkeys(mongo_db, mongo_appkey_table, mongoClient)
              appkeys_broad.unpersist()
              appkeys_broad = ssc.sparkContext.broadcast(appkeys_update)
              val appkeys_pattern_update = MongoDBUtil.getAppkeyPattern(mongo_db, mongo_reg_table, mongoClient)
              appkeys_pattern_broad.unpersist()
              appkeys_pattern_broad = ssc.sparkContext.broadcast(appkeys_pattern_update)
              val appkeys_urls_update = MongoDBUtil.getAppkeyUrl(mongo_db, mongo_doctor_table, mongoClient, appkeys_pattern)
              appkeys_urls_broad.unpersist()
              appkeys_urls_broad = ssc.sparkContext.broadcast(appkeys_urls_update)
              val searchEngine_update = MongoDBUtil.getSearchEngine(mongo_db, mongo_searchengine_table, mongoClient)
              searchEngine_broad.unpersist()
              searchEngine_broad = ssc.sparkContext.broadcast(searchEngine_update)

            } catch {
              case e: Throwable => LOG.error(exceptionToString(e))
            }
          }
          LOG.info("conf update thread stopped")

        }

      }
      confThread.start()

      val partitonOffset_stream = ProcessedOffsetManager.getPartitionOffset(tmp_stream)
      //filter method PageView、EndPageView、LinkClick.PageView计算uv，pv、ip指标，PageView和EndPageView计算每个session的详细信息，PageView和LinkClick计算页面医生相关的指标
      val lines = tmp_stream.map { x => new String(x.getPayload) }.filter { x => BDIFunction.filter_source(x, methods, hasprefix, accu) }
      lines.cache()
      val appkey_time = lines.map { x => BDIFunction.extract_stat(x, appkeys_broad.value, hasprefix) }.filter(x => x._1 != "").map(x => (x._1 + "\t" + x._4, x._2 + "\t" + x._3))
      appkey_time.groupByKey().foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          //获取连接，使用单例模式
          val mongoClient = MongodbPool.getMongoClient(mongo_url)
          val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          partition.foreach(data => {
            val appkey_time = data._1.split("\t")
            val appkey = appkey_time(0)
            val time = appkey_time(1).toLong
            //根据appkey和time获取历史数据，如果没有则插入，更新pv ，uv，ip
            val (flag, history, collection) = MongoDBUtil.findStatByAppkeyAndTime(mongo_db, mongo_stat_table, mongoClient, appkey, time)
            if (flag) {
              //update
              val ip_gids = data._2
              val uvCard = new AdaptiveCounting(history.get(Constant.MONGO_FIELD_UV_CARD).asInstanceOf[Binary].getData)
              val ipCard = new AdaptiveCounting(history.get(Constant.MONGO_FIELD_IP_CARD).asInstanceOf[Binary].getData)
              var pv = history.getLong(Constant.MONGO_FIELD_PV)
              for (ip_gid <- ip_gids) {
                pv += 1
                val fields = ip_gid.split("\t")
                uvCard.offer(fields(0))
                ipCard.offer(fields(1))
              }
              val updateCondition = new Document()
                .append(Constant.MONGO_FIELD_APPKEY, appkey)
                .append(Constant.MONGO_FIELD_DATETIME, time)
              val updateValue = new Document()
                .append(Constant.MONGO_FIELD_PV, pv)
                .append(Constant.MONGO_FIELD_UV, uvCard.cardinality())
                .append(Constant.MONGO_FIELD_IP_COUNT, ipCard.cardinality())
                .append(Constant.MONGO_FIELD_UV_CARD, uvCard.getBytes)
                .append(Constant.MONGO_FIELD_IP_CARD, ipCard.getBytes)
                .append(Constant.MONGO_FIELD_UPDATE_STAMP, DATE_FORMAT.format(new Date()))
              collection.updateOne(updateCondition,
                new Document("$set", updateValue),
                new UpdateOptions().upsert(true))

            } else {
              //insert
              val ip_gids = data._2
              var pv = 0L
              val uvCard = new AdaptiveCounting(card_init_bits)
              val ipCard = new AdaptiveCounting(card_init_bits)
              for (ip_gid <- ip_gids) {
                pv += 1
                val fields = ip_gid.split("\t")
                uvCard.offer(fields(0))
                ipCard.offer(fields(1))
              }
              val insert = new Document()
                .append(Constant.MONGO_FIELD_PV, pv)
                .append(Constant.MONGO_FIELD_UV, uvCard.cardinality())
                .append(Constant.MONGO_FIELD_IP_COUNT, ipCard.cardinality())
                .append(Constant.MONGO_FIELD_UV_CARD, uvCard.getBytes)
                .append(Constant.MONGO_FIELD_IP_CARD, ipCard.getBytes)
                .append(Constant.MONGO_FIELD_UPDATE_STAMP, DATE_FORMAT.format(new Date()))
              val updateCondition = new Document()
                .append(Constant.MONGO_FIELD_APPKEY, appkey)
                .append(Constant.MONGO_FIELD_DATETIME, time)
              collection.updateOne(updateCondition,
                new Document("$set", insert),
                new UpdateOptions().upsert(true))
            }
          })

        })
      })
      val details = lines.map { x => BDIFunction.extract_detail(x, ipLookUp_broad.value, appkeys_broad.value, ip_Method_broad.value, hasprefix) }.filter(x => x._1 != "0")
      val begin = details.map(x => (x._2 + "\t" + x._3, (x._1, x._4)))
      begin.groupByKey().foreachRDD(rdd => {

        rdd.foreachPartition(partition => {
          //获取连接，使用单例模式
          val mongoClient = MongodbPool.getMongoClient(mongo_url)
          val collection = mongoClient.getDatabase(mongo_db).getCollection(mongo_detail_table)
          import scala.collection.JavaConversions._
          partition.foreach(data => {
            val appkey_sid = data._1.split("\t")
            val appkey = appkey_sid(0)
            val sid = appkey_sid(1)
            val historys = collection.find(new Document().append(Constant.MONGO_FIELD_APPKEY, appkey).append(Constant.MONGO_FIELD_SID, sid))
            var history: Document = null
            if (!historys.isEmpty) {
              history = historys.first()
            }
            val type_datas = data._2
            val detailInfoValue = BDIFunction.updateDetail(history, type_datas)
            val channel_type = SearchEngine.getLinkType(detailInfoValue.getLanding_page(), detailInfoValue.getLn_page(), searchEngine_broad.value)
            detailInfoValue.setChannel_type(channel_type)
            if (channel_type == Constant.BDI_MEDIA_LINK_TYPE_SEARCH) {
              detailInfoValue.setChannel(SearchEngine.getSearchEngineName(detailInfoValue.getLn_page(), searchEngine_broad.value))
            } else if (channel_type == Constant.BDI_MEDIA_LINK_TYPE_EXTERNAL) {
              detailInfoValue.setChannel(SearchEngine.getUrlHost(detailInfoValue.getLn_page()))
            } else {
              detailInfoValue.setChannel("直接访问")
            }
            MongoDBUtil.updateDetail(detailInfoValue, collection)
          })
        })
      })

      val pagedoctor = lines.map { x => BDIFunction.extract_pagedoctor(x, appkeys_pattern_broad.value, appkeys_urls_broad.value, hasprefix) }.filter(x => x._1 != "0")
      val pv = pagedoctor.map(x => (x._2 + "\t" + x._3 + "\t" + x._4, x._5 + "\t" + x._6 + "\t" + x._7 + "\t" + x._8))
      pv.groupByKey().foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val hTable = HbasePool.getHbaseClient(hbase_zookeeper_quorum, zookeeper_znode_parent, hbase_rootdir, hbase_zookeeper_porperty_clientPort, hbase_table)
          partition.foreach(data => {
            val key = data._1
            val fields = key.split("\t")
            val url = fields(0)
            val appkey = fields(1)
            val method = fields(2)
            val infos = data._2
            if (method.equals("PageView")) {
              for (info <- infos) {
                val year = info.split("\t")(0)
                val hbase_key = year + ":" + appkey + ":pv:" + url
                hTable.incrementColumnValue(Bytes.toBytes(DataUtil.string2MD5(key) + ":" + key), Bytes.toBytes("statistic"), Bytes.toBytes("pv"), 1)
              }
            } else {
              //("2", uniformUrl, customer, "LinkClick", element, times.get("year").get, times.get("hour").get, ln)
              for (info <- infos) {
                val segs = info.split("\t")
                val element = segs(0)
                val year = segs(1)
                val hour = segs(2)
                val ln = segs(3)
                val key1 = year + ":" + appkey + ":all:" + url
                val key2 = year + ":" + appkey + ":" + url + ":" + element
                val key4 = year + ":" + appkey + ":" + url
                hTable.incrementColumnValue(Bytes.toBytes(DataUtil.string2MD5(key1) + ":" + key1), Bytes.toBytes("statistic"), Bytes.toBytes(element), 1)
                hTable.incrementColumnValue(Bytes.toBytes(DataUtil.string2MD5(key4) + ":" + key4), Bytes.toBytes("statistic"), Bytes.toBytes(hour + ":" + element), 1)
                hTable.incrementColumnValue(Bytes.toBytes(DataUtil.string2MD5(key2) + ":" + key2), Bytes.toBytes("statistic"), Bytes.toBytes(hour + ":" + ln), 1)
              }
            }
          })
        })
      })

      ProcessedOffsetManager.persists(partitonOffset_stream, props)
      ssc.start()
      ssc.awaitTermination()
    }

    //background thread for topic switch

    class SwitchTask extends TimerTask {
      override def run() {
        LOG.info("stop last day")
        stopLastDay()
        LOG.info("register task")
        registerTask()
        LOG.info("start ssc")
        startStreamContext()
      }
    }

    def registerTask() {
      timer = new Timer()
      schedule = new TimeSchedule(timer)
      schedule.addFixedTask(hour, minute, 0, new SwitchTask)
    }
    registerTask
    startStreamContext()
  }

  //exception to string
  def exceptionToString(e: Throwable) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw, true)
    e.printStackTrace(pw)
    pw.flush()
    sw.flush()
    sw.toString()
  }

}