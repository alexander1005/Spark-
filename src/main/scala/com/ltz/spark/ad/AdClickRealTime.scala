package com.ltz.spark.ad

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.ltz.spark.UserVisitSessionAnaly.CategorySortKey
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广告流量实时统计
  */
object AdClickRealTime {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

 //   StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    //构建spark streaming
    val conf = new SparkConf().setAppName("ad").setMaster("local[2]")
      .registerKryoClasses(Array(CategorySortKey.getClass))
    //  .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
     // .set("spark.storage.memoryFraction","0.4")
    val sc = new SparkContext(conf)
    //构建 streamingContext 传入sc,实时处理bactch的interval
    //每隔一段时间会 收集数据源中的数据，做成一个batch
    //每次处理一个batch中的数据， 一般设置数s 到数10s
    val streamingContext = new StreamingContext(sc,Durations.seconds(5))
    streamingContext.start()
    streamingContext.awaitTermination()

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaParams, topicsSet)
    //拿到一条消息日志
    //timestamp province city userid adid
    //计算每5s中每天每个用户每个广告点击量
    val adId = messages.map(t=>{
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val strings = t._2.split(" ")
      val date = sdf.format(new Date(strings(0).toLong))
      val userId = strings(3)
      val adid = strings(4)
      (date+"_"+userId+"_"+adid,1)
    }).reduceByKey(_+_)
    adId.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionCord=>{
          val connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/sparksession","root","root")
          connection.setAutoCommit(false)
          val sdf = new SimpleDateFormat("yyyyMMdd")
          val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
          val statement = connection.prepareStatement(" INSERT INTO ad_user_click_count  VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE user_id=?,date=?,ad_id=?     ")
          partitionCord.foreach(_ match {case(v1,count)=>{
            val strings = v1.split("_")
            val date = sdf1.format(sdf.parse(strings(0)))
            val userId = strings(1).toInt
            val adId = strings(2).toInt
            statement.setString(0,date)
            statement.setInt(1,userId)
            statement.setInt(2,adId)
            statement.setInt(3,count)
            statement.setInt(4,userId)
            statement.setString(5,date)
            statement.setInt(6,adId)
            statement.addBatch()
          }})
          statement.executeBatch()
          connection.commit()
          connection.close()
      })
    })

    streamingContext.stop()
  }

}
