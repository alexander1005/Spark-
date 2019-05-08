package com.ltz.spark.page

import java.sql.DriverManager

import com.ltz.spark.UserVisitSessionAnaly
import kafka.utils.Json
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.ltz.spark.Constants
import com.ltz.spark.util._
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable
import scala.io.Source

object PageOneStepRateSpark {


  val generateAndMatchPageSplit = (sc: SparkContext, rdd: RDD[(String, Iterable[Row])], param: String) => {

    val targetPageFlow = "1,2,3,4,5,6,7,8,9";
    val targetPage = sc.broadcast(targetPageFlow)
    rdd.flatMap(r => {
      val targetPages = targetPage.value.toString.split(",")
      val lst = mutable.MutableList[(String, Int)]()
      val sessionId = r._1
      val rows = r._2.toList.sortBy(row => DateUtils.parseTime(row.getString(4)))
      var lastPageId = -999999l
      for (rs <- rows) {
        val pageId = rs.getLong(3)
        if (lastPageId == -999999l) {
          lastPageId = pageId
        } else {
          // 生成一个页面切片
          // 3,5,2,1,8,9
          // lastPageId=3
          // 5，切片，3_5
          val pageSplit = lastPageId + "_" + pageId
          // 对这个切片判断一下，是否在用户指定的页面流中
          for (i <- 1 to targetPages.length - 1) {
            val index = i - 1
            val targetPageSplit = targetPages(index) + "_" + targetPages(i)
            if (pageSplit == targetPageSplit) {
              lst.+=((pageSplit, 1))
            }
          }
          lastPageId = pageId
        }
      }
      lst
    })
  }


  def getActionRddByParam(param: String, sqlSc: SQLContext) = {
    val sql = "select" +
      " *" +
      " from " +
      " user_visit_action   "
    sqlSc.sql(sql)
  }

  case class Page(targetPageFlow:String,startDate:String,endDate:String)

  /**
    * 获取页面起始流量pv
    *
    * @param param
    * @param sessionRdd
    */
  def getStartPagePv(param: String, sessionRdd: RDD[(String, Iterable[Row])]) = {
    val targetPageFlow = Json.parseFull(param).getOrElse(Constants.PARAM_TARGET_PAGE_FLOW).toString
   // val js =Json.parseFull(param).map(t=>t).take(1)(0)
    //val mapper = new ObjectMapper()
    val p = "1,2,3,4,5,6,7,8,9"
    val startPageId = p.split(",")(0).toLong
    val startPageRdd = sessionRdd.flatMap(r => {
      val lst = mutable.MutableList[Long]()
      r._2.foreach(row => {
        val pageId = row.getLong(3)
        if (pageId == startPageId) {
          lst += pageId
        }
      })
      lst
    })
    startPageRdd.count()
  }

  def computePageSplitConvertRate(pageSplitPvMap: collection.Map[String, Long], startPagePv: Long,param:String) = {
    val convertRateMap  = mutable.HashMap[String,Double]()
    val targetPages = Json.parseFull(param).
      getOrElse( Constants.PARAM_TARGET_PAGE_FLOW)
      .toString.split(",")
    var lastPageSplitPv = 0l
    //3,5,2,4,6
    //3_5
    //转化率：3_5 pv /3 pv
    //5_2 rate=5_2 pv/3_5 pv
    //通过for循环，获取目标页面流中的各个页面切片（pv）
    for(i <- 1 to targetPages.length-1 ){
      val targetPageSplit=targetPages(i-1)+"_"+targetPages(i)
      val targetPageSplitPv = pageSplitPvMap(targetPageSplit)
      var convertRate = 0.0
      if (i ==1 ){
        convertRate = NumberUtils.formatDouble(targetPageSplitPv/startPagePv,2)
      }else{
        convertRate = NumberUtils.formatDouble(targetPageSplitPv/lastPageSplitPv,2)

      }
      convertRateMap(targetPageSplit)  = convertRate
      lastPageSplitPv = targetPageSplitPv

    }
    convertRateMap
  }

  def persistConvertRate(taskId: String, tartgetRate: mutable.HashMap[String, Double]): Unit = {
    val buffer = ""
    tartgetRate.foreach(entry=>{
      buffer+ s"${entry._1}=${entry._2}|"
    })
    val convertRate = buffer.substring(0,buffer.length-1)
    //insert taskid convertRate
  }

  def main(args: Array[String]): Unit = {

    //1.构造spark上下文

    val conf = new SparkConf().setAppName("PageOneStep").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = UserVisitSessionAnaly.sqlcontext(sc)
    //2.生成模拟数据
    UserVisitSessionAnaly.mockData(sc, sqlSc)
    //3.查询任务，获取任务参数
    val taskId = "2"
    val conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/sparksession", "root", "root")
    val statement = conn.createStatement()
    val res = statement.executeQuery(s"select * from task where task_id = '$taskId'");
    res.next()
    val param = res.getString("task_param")
    res.close()
    statement.close();
    conn.close();
    //4.查询指定日期范围的用户访问行为数据
    val rdd = getActionRddByParam(param, sqlSc)
    val actionRdd = rdd.mapPartitions(row => {
      row.map(r => {
        (r.getString(2), r)
      })
    }).cache()
    val sessionRdd = actionRdd.groupByKey()
    val pageMacthRdd = generateAndMatchPageSplit(sc, sessionRdd, param)
    //每个session的单跳页面切片的生成以及页面流匹配
    val pageSplitPvMap = pageMacthRdd.countByKey()
    //使用者指定的页面流是3,2,5,8,6
    //咱们现在拿到的这个pageSplitPvMap就是3->2,2->5,5->8,8->6
    val startPagePv =  getStartPagePv(param, sessionRdd)
    //目标单跳页面转化率
    val tartgetRate = computePageSplitConvertRate(pageSplitPvMap, startPagePv, param)
    println(tartgetRate)
    persistConvertRate(taskId,tartgetRate)
  }

}
