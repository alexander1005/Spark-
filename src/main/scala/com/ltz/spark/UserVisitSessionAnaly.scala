package com.ltz.spark

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ltz.spark.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.Random

/**
  *
  *
  * 接受用户创建的任务，用户可指定范围
  *
  * 1.时间范围：起始日期 结束日期
  * 2.性别  男或者女
  * 3。年龄 范围
  * 4。职业 多选
  * 5。城市 多选
  * 6。搜索词 多个  任何一个action 搜索过 就符合
  * 7。点击品类，任何一个action 搜索过 就符合
  */
object UserVisitSessionAnaly {

  val sqlcontext = (sc: SparkContext) => {
    if (Constants.LOCAL) {
      new SQLContext(sc)
    }
    else
      new HiveContext(sc)
  }


  def AggrSessionVisitLen(visitLen: Long, sessionAggr: Accumulator[String]) = {
    if (visitLen > 0 && visitLen <= 3) {
      sessionAggr.add(Constants.STEP_PERIOD_1_3)
    }
    if (visitLen > 3 && visitLen <= 6) {
      sessionAggr.add(Constants.STEP_PERIOD_4_6)
    }
    if (visitLen > 6 && visitLen <= 9) {
      sessionAggr.add(Constants.STEP_PERIOD_7_9)
    }
    if (visitLen > 9 && visitLen <= 30) {
      sessionAggr.add(Constants.STEP_PERIOD_10_30)
    }
    if (visitLen > 30 && visitLen <= 60) {
      sessionAggr.add(Constants.STEP_PERIOD_30_60)
    }
    if (visitLen > 60) {
      sessionAggr.add(Constants.STEP_PERIOD_60)
    }
    //............

  }

  def AggrSessionStepLen(step: Int, sessionAggr: Accumulator[String]) = {
    if (step > 0 && step <= 3) {
      sessionAggr.add(Constants.STEP_PERIOD_1_3)
    }
    //............

  }

  def getFieldsFromString(info: String, split: String, const: String): String = {

    for (n <- info.split(split)) {
      if (n.split("=")(0) == const)
        return n.split("=")(1)
    }
    return ""
  }

  def filterData(info: String, fields: Array[String], startAge: Int, endAge: Int, professionals: String, city: String, sex: String, keywords: String, categoryIds: String, sessionAggr: Accumulator[String]): Boolean = {

    val spertor = "\\|"
    //过滤条件
    //    for (field <- fields) {
    //      val params = spertor.split("=")
    //      val param = params(0)
    //      val value = params(1)
    //      if (param == Constants.FIELDS_USER_AGE) {
    //        if (value.toInt > endAge || value.toInt < startAge) {
    //          return false
    //        }
    //      }
    //    }
    //该session是合法的
    sessionAggr.add(Constants.SESSION_COUNT)
    //根据session访问时长和步长累加
    var visitLen = 0l
    val vtmp = getFieldsFromString(info, "\\|", Constants.FIELD_VISIT_LENGTH)
    if (vtmp != "") {
      visitLen = vtmp.toLong
    }
    val stmp = getFieldsFromString(info, "\\|", Constants.FIELD_STEP_LENGTH)
    var stepLen = 0
    if (stmp != "") {
      stepLen = stmp.toInt
    }
    AggrSessionVisitLen(visitLen, sessionAggr)
    AggrSessionStepLen(stepLen, sessionAggr)
    true
  }

  /**
    * 获取top n 热门品类
    *
    * @param res
    * @param sessionInfo
    */
  def getTopNCateGory(res: RDD[(String, String)], sessionInfo: RDD[(String, Row)]): Array[CategorySortKey] = {

    //过滤session后的详细数据,
    val sessionDetail = res.join(sessionInfo).map(t => (t._1, t._2._2))
    //获取session访问过的品类id 算出所有的商品
    // 点击过，下单过，支付过的品类
    val categoryid = sessionDetail.flatMap(t => {
      val row = t._2
      val list = mutable.MutableList[(Long, Long)]()
      var categoryId = row.get(6)
      if (categoryId != null) {
        val id = categoryId.toString.toLong
        list += ((id, id))
      }
      //下单
      var orderId = row.getString(8)
      if (orderId != null && orderId != "") {
        val orders = orderId.split(",")
        orders.foreach(oId =>
          if (oId != "") {
            list += ((oId.toLong, oId.toLong))
          }
        )
      }
      //支付
      var payId = row.getString(10)
      if (payId != null && orderId != "") {
        val pays = payId.split(",")
        pays.foreach(pid => if (pid != "") {
          list += ((pid.toLong, pid.toLong))
        })
      }
      list
    }).distinct()

    //访问明细 三种访问行为 点击 下单 支付
    //分别来计算各个品类点击、下单和支付次数。可以先对访问明细数据进行过滤
    //分别过滤出点击、下单、支付行为，然后通过map reducebykey等算子计算
    //计算各个品类点击次数

    val clickRdd = sessionDetail.filter(t => {
      val row = t._2;
      //点击
      val click = row.get(6)
      if (click != null && click.toString.trim != "") {
        true
      } else {
        false
      }
    })
    val clickCagoryId = clickRdd.map(t => {
      val clickCagteId = t._2.getLong(6)
      (clickCagteId, 1l)
    })
    //计算点击次数
    /**
      * 计算品类点击次数，如何某个品类点击1000万，某个才1万。出现数据倾斜
      *
      *
      */
   //优化一 val clickCountRdd = clickCagoryId.reduceByKey((v1, v2) => v1 + v2,1000)
   //val clickCountRdd = clickCagoryId.reduceByKey((v1, v2) => v1 + v2,1000);

   //优化二， 随机key实现双重聚合,加随机数 然后去掉
     val mapRdd = clickCagoryId.map(row=>{
       val r = Random.nextInt(10)
       (r+"_"+row._1,row._2)
     })


    val fist = mapRdd.reduceByKey(_+_)
    val clickCountRdd =fist.map(row=> {

      (row._1.split("_")(1).toLong, row._2)
    }).reduceByKey(_+_)


    //下单次数
    val orderRdd = sessionDetail.filter(t => {
      val row = t._2;
      //点击
      val click = row.get(8)
      if (click != null && click.toString.trim != "") {
        true
      } else {
        false
      }
    })
    val orderCagoryId = orderRdd.flatMap(t => {
      val categoryid = t._1
      val orders = t._2.getString(8)
      val ids = orders.split(",")
      val list = mutable.MutableList[(Long, Long)]()
      ids.foreach(id => {
        if (id != "") {
          list += ((id.toLong, 1l))
        }
      })
      list
    })
    //计算下单次数
    val orderCountRdd = orderCagoryId.reduceByKey((v1, v2) => v1 + v2)


    //支付次数
    val payRdd = sessionDetail.filter(t => {
      val row = t._2;
      //点击
      val click = row.get(8)
      if (click != null && click.toString.trim != "") {
        true
      } else {
        false
      }
    })
    val payCagoryId = payRdd.flatMap(t => {
      val categoryid = t._1
      val orders = t._2.getString(10)
      val ids = orders.split(",")
      val list = mutable.MutableList[(Long, Long)]()
      ids.foreach(id => {
        if (id != "") {
          list += ((id.toLong, 1l))
        }
      })
      list
    })
    //计算支付次数
    val payCountRdd = orderCagoryId.reduceByKey((v1, v2) => v1 + v2)

    /**
      * join 各品类与它的点击，下单和支付次数
      *
      * 返回  category , categoryid=12|click=20|order=15|pay=10
      */
    val categoryCountRdd = categoryid.leftOuterJoin(clickCountRdd).map(_ match {
      case (categoryId, countOp) => {
        val count = countOp._2.getOrElse(0l)
        (categoryId, s"${Constants.FIELDS_CATEGORY_ID}=${categoryId}|${Constants.FIELDS_CLICK_COUNT}=${count}|")
      }
    }).leftOuterJoin(orderCountRdd).map(_ match {
      case (categoryId, orderOp) => {
        val other = orderOp._1
        val count = orderOp._2.getOrElse(0l)
        (categoryId, s"${other}|${Constants.FIELDS_ORDER_COUNT}=${count}|")
      }
    }).leftOuterJoin(payCountRdd).map(_ match {
      case (categoryId, payOp) => {
        val other = payOp._1
        val count = payOp._2.getOrElse(0l)
        (categoryId, s"${other}|${Constants.FIELDS_PAY_COUNT}=${count}")
      }
    })

    /**
      *
      *
      * 自定义 二次排序
      *
      * 根据点击、下单支付 、二次排序
      *
      */
    /**
      *
      *
      * 将数据映射成《SortKey,info》格式的rdd,然后进行二次排序
      *
      */

    val sortRdd = categoryCountRdd.map(_ match {
      case (id, value) => {
        val cateId = getFieldsFromString(value,"\\|",Constants.FIELDS_CATEGORY_ID).toLong
        val click = getFieldsFromString(value,"\\|",Constants.FIELDS_CLICK_COUNT).toLong
        val order = getFieldsFromString(value,"\\|",Constants.FIELDS_ORDER_COUNT).toLong
        val pay = getFieldsFromString(value,"\\|",Constants.FIELDS_PAY_COUNT).toLong
        (CategorySortKey(cateId,click,order,pay),0)
      }
    }).sortByKey(false)

    /**
      * 取出topN 写入mysql
      *
      */
//    CategorySortKey(99,21,58,58)
//    CategorySortKey(24,21,55,55)
//    CategorySortKey(49,21,53,53)
//    CategorySortKey(50,21,43,43)
//    CategorySortKey(77,21,42,42)
//    CategorySortKey(73,20,55,55)
//    CategorySortKey(83,20,50,50)
      sortRdd.take(10).map(_._1)
  }

  case class CategorySortKey(cateId:Long,click: Long, order: Long, pay: Long) extends Ordered[CategorySortKey] {

    override def compare(that: CategorySortKey): Int = {

      if (click - that.click != 0) {
        (click - that.click).toInt
      } else if (order - that.order != 0)
        (order - that.order).toInt
      else
        (pay - that.pay).toInt
    }

    override def >(that: CategorySortKey): Boolean = {
      if (click > that.click)
        true
      else if (order > that.order)
        true
      else if (pay > that.pay)
        true
      else
        false
    }
  }


  /**
    * top10 热门商品 活跃
    * @param sessionDetail
    * @param categorySortKeys
    * @return
    */
  def getTop10Session(sc: SparkContext,sessionDetail: RDD[(String, Row)], categorySortKeys: Array[CategorySortKey]) = {

    val rdd = sessionDetail.groupByKey().flatMap(_ match {case (id,iter)=>{
      val categoryMap:mutable.HashMap[Long,Long] = mutable.HashMap()
      iter.foreach(row=>{

        if(row.get(6)!=null){
           val id = row.getLong(6)
          if (!categoryMap.contains(id)){
            categoryMap(id) = 0
          }
          val count = categoryMap(id)
          categoryMap(id) = count+1

          if(categoryMap(id)>=2){
            println("--------------------------------------")
            println(s"id: ${id}  count = ${categoryMap(id)}")
          }
        }
      })
      val list  = mutable.MutableList[(Long, String)]()
      categoryMap.foreach(t=>{
        val cateroyid = t._1
        val count = t._2
        list += ((cateroyid,id+","+count))
      })
      list
    }})
    val list = categorySortKeys
      .map(sort=>(sort.cateId,sort.cateId)).toList
    val categoryRdd = sc.makeRDD(list)  //categoryid ,categoryid    Long long
     // rdd    categoryId, sessionid+count  : Long String

    rdd.join(categoryRdd).map(t=>{ // long (String,Long)
      (t._1,t._2._1)
    }).groupByKey().flatMap(_ match {case (categoryId,iter)=>{
      val top10Seesion =  mutable.MutableList[(Long,String)]()
      iter.foreach(row=>{
        val sessionId = row.split(",")(0)
        val count = row.split(",")(1).toLong
        top10Seesion+=((count,sessionId))
      })
      println(categoryId +","+top10Seesion.sorted.reverse.take(10))
      println("----------------------------")
      top10Seesion.sorted.take(10)
    }}).count()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("visit").setMaster("local[2]")
      .registerKryoClasses(Array(CategorySortKey.getClass))
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.storage.memoryFraction","0.4")
    val sc = new SparkContext(conf)
    val sqlContext = sqlcontext(sc)
    //生成模拟数据
    mockData(sc, sqlContext)
    //session聚合，首先从user_visit_action 查询指定日期范围


    //    * 1.时间范围：起始日期 结束日期
    //      * 2.性别  男或者女
    //      * 3。年龄 范围
    //      * 4。职业 多选
    //      * 5。城市 多选
    //      * 6。搜索词 多个  任何一个action 搜索过 就符合
    //    * 7。点击品类，任何一个action 搜索过 就符合

    //查询指定任务
    val taskId = ParamUtils.getTaskIdFromArgs(args)
    //    val res = JDBCHelper.getConnection.
    //      createStatement().
    //      executeQuery("select task_param from task where task_id=" + taskId)
    //    res.next()
    //    val param = res.getString("task_param")
    //    res.close()
    //val dataFrame = getActionRDDByDataRange(sqlContext,JSON.parseObject(param))
    //val taskParms = JSON.parseObject(param)
    //提取参数
    val dataFrame = getActionRDDByDataRange(sqlContext, null)


    // 根据 session_id 进行分组,此时数据粒度成了session粒度 与用户进行join
    //session粒度
    val sessionInfo = dataFrame.map(row => (row.getString(2), row)).persist(StorageLevel.MEMORY_ONLY)
    val session = sessionInfo.groupByKey().cache()


    //聚合关键词与商品
    val userSessionInfo = session.map(items => {
      val sessionId = items._1
      val iterable = items._2
      val searchBuilder = new StringBuilder()
      val categoryBuilder = new StringBuilder()
      var userId = 0l
      var startTime: Date = null
      var endTime: Date = null
      var stepLength: Int = 0
      iterable.map(row => {
        //只有搜索行为有searchKeyWORD
        if (userId == 0) {
          userId = row.getLong(1)
        }

        val searchKeyWord = row.getString(5)
        //只有点击行为有click
        val clickGategoryId = row.get(6)

        val actionTime = DateUtils.parseTime(row.getString(4))
        //数据有null
        if (startTime == null) {
          startTime = actionTime
        }
        if (endTime == null) {
          endTime = actionTime
        }
        if (actionTime != null && actionTime.before(startTime)) {
          startTime = actionTime
        }
        if (actionTime != null && actionTime.after(endTime)) {
          endTime = actionTime
        }
        if (startTime == null) {
          startTime = new Date()
        }
        stepLength = stepLength + 1
        if (StringUtils.isNotBlank(searchKeyWord)
          && !searchBuilder.contains(searchKeyWord)) {
          searchBuilder.append(searchKeyWord).append(",")
        }
        if (clickGategoryId != null &&
          !categoryBuilder.contains(clickGategoryId)) {
          categoryBuilder.append(clickGategoryId).append(",")
        }

      })
      if (searchBuilder.endsWith(","))
        searchBuilder.setLength(searchBuilder.length - 1)
      if (categoryBuilder.endsWith(","))
        categoryBuilder.setLength(categoryBuilder.length - 1)

      val visitTime = (endTime.getTime - startTime.getTime) / 1000
      val category = String.valueOf(categoryBuilder)
      val searchword = String.valueOf(searchBuilder)
      // key=values|key=values
      val finalItems = Constants.FIELDS_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELDS_SEARCH_KEYWORDS + "=" + searchword + "|" +
        Constants.FIELDS_CLICK_CATEGORYID + "=" + category + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitTime + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + startTime.toLocaleString
      (userId.toString, finalItems)

    }).cache()
    println("----------------------agger-----------------------")


    val sessionAggr = sc.accumulator("")(SessionAggrStatAccumular)


    //查询用户数据
    val userSql = "select * from user_info"
    val userRdd = sqlContext.sql(userSql).map(row => (row.getLong(0), row))
      .groupByKey().map(t => (t._1.toString,t._2.toBuffer(0))) //str row
    val userSessionFull = userSessionInfo.join(userRdd)

    val finalRes = userSessionFull.map(tuple => {
      val userId = tuple._1
      val userInfo = tuple._2._2
      val sessionInfo = tuple._2._1

      //sessionId
      val sessionId = getFieldsFromString(sessionInfo, "\\|", Constants.FIELDS_SESSION_ID)

      val age = userInfo.getInt(3)
      val sex = userInfo.getString(6)
      val city = userInfo.getString(5)
      val professional = userInfo.getString(4)

      val user = sessionInfo + "|" +
        Constants.FIELDS_USER_AGE + "=" + age + "|" +
        Constants.FIELDS_USER_PROFESSION + "=" + professional + "|" +
        Constants.FIELDS_USER_CITY + "=" + city + "|" +
        Constants.FIELDS_USER_SEX + "=" + sex
      (sessionId, user)
    }).cache()
    //session info | user info
    val startAge = 12
    val endAge = 60
    val professionals = ""
    val city = ""
    val sex = ""
    val keywords = "火锅"
    val categoryIds = ""
    val res = finalRes.filter(tuple => {
      val aggrinfo = tuple._2;
      //年内
      val fields = aggrinfo.split("\\|")
      filterData(aggrinfo, fields, startAge, endAge, professionals, city, sex, keywords, categoryIds, sessionAggr)
    })

    //计算session各个小时的数量
    //此处可能是出现数据倾斜的地方，
    val timeSession = finalRes.map(t => {

      val aggrInfo = t._2
      val startTime = getFieldsFromString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dataHour = DateUtils.getDateHour(startTime);
      (dataHour, t._2)
    })

    val dateHourMap: HashMap[String, HashMap[String, Long]] = HashMap()


    timeSession.countByKey().foreach((k) => {
      val dataHour = k._1
      val date = dataHour.split("_")(0)
      val hour = dataHour.split("_")(1)
      if (!dateHourMap.contains(date))
        dateHourMap(date) = HashMap()
      dateHourMap(date)(hour) = k._2
    })

    val extract = 100 / dateHourMap.size

    val dataHoursCount: HashMap[String, HashMap[String, mutable.MutableList[Int]]] = HashMap()
    for (i <- dateHourMap) {
      val day = i._1
      val hourCountMap = i._2
      if (!dataHoursCount.contains(day)) {
        dataHoursCount(day) = HashMap()

      }
      val dcount: Long = hourCountMap.values.sum

      for (j <- hourCountMap) {

        val hour = j._1
        val hcount = j._2
        //计算每个小时session数量，占总session比例

        if (!dataHoursCount(day).contains(hour)) {
          dataHoursCount(day)(hour) = mutable.MutableList()
        }
        var num = 0
        if (hcount / dcount * extract == 0) {
          num = 1
        } else {
          num = (hcount / dcount * extract).toInt
        }
        println(s"num = ${num}")
        for (n <- 1 to num) {
          val index = Random.nextInt(hcount.toInt)
          dataHoursCount(day)(hour) = dataHoursCount(day)(hour) += index
        }
        println(s" dataHoursCount(day)(hour) =  day : ${day}  hour : ${hour}")
      }
    }
    val broadMap = sc.broadcast(dataHoursCount)
    val rdd = timeSession.groupByKey().flatMap(t => {
      val day = t._1.split("_")(0)
      val hour = t._1.split("_")(1)
      val iter = t._2
      val dataHoursCount1 = broadMap.value
      var index: Int = 0
      val list = mutable.MutableList[(String, String)]()
      iter.foreach(sessionAggrInfo => {
        val flag = dataHoursCount1(day)(hour).contains(index)
        val sesionId = getFieldsFromString(sessionAggrInfo, "\\|", Constants.FIELDS_SESSION_ID)
        if (flag) {
          println("-----------抽取sessionAggrInfo---------------")
          println(sessionAggrInfo)
          list += ((sesionId, sesionId))
          index += 1
        }
      })
      list
    }).map(t => (t._1, t._2))
    //插入数据库 随机抽取结果
    println(rdd.count())
    /*
    *
    *  session 随机抽取
    *
    * 第零步 统计时间，计算出 ( yyyy-dd-mm_HH ,List(session)) 算子
    * 第一步 统计时间，根据day来均匀分配每天要抽取段数量
    * 第二步 统计每天每个小时，遍历每天每个小时,（当前小时session数量/当天seesion数量）*抽取数量
    * 第三步 得到数量之后，进行遍历，根据每天每小时生成 数量 个 随机index, 但index 不能大于当前小时session数量
    * 第四步 遍历( yyyy-dd-mm_HH ,List(session))提取出 yyyy-mm-dd  hh 两个参数，从第三步生成的map获取session
    * 第五步 join详细数据，得到用户与session详情写入数据库
    *
    *
    * */
    rdd.join(finalRes).foreachPartition(t => {
      t.foreach(f => {
        println(s"sessionId : ${f._1}")
        println(s"sessionId : ${f._2._1}")
        println(s"sessionInfo : ${f._2._2}")
        //insert mysql
        //随机抽取的session
      })
    })


    /**
      *
      * 统计访问步长，时长，以及session过滤.,用户条件, 自定义agger
      *
      * 第一步 将用户访问数据，聚合成session粒度 group by key，并以指定的格式   (userId,sessionInfo)
      * 第二步 清理用户数据，以指定的格式(userId,userInfo )
      * 第三步 join  得到 (userId,sessionInfo,userInfo)
      * 第四步 遍历join的数据，并使用自定义的agger ，过滤参数，条件,搜索指标，agger自增。
      * 第五部 得到过滤后的数据 写入mysql
      */
    val session_Count = getFieldsFromString(sessionAggr.value, "\\|", Constants.SESSION_COUNT)
    val step_1_3_count = getFieldsFromString(sessionAggr.value, "\\|", Constants.STEP_PERIOD_1_3)
    //val precent = step_1_3_count.toLong / session_Count.toLong

    /*
    *
    * topN 热门商品类
    *
    * 计算出来通过条件筛选的session,他们访问过的所有的品类，(点击，下单，支付)，
    * 按照各个品类的点击，下单和支付次数。降序排序，获取前N个品类，
    *
    * 按照 点击，下单 ，支付 依次降序排序（自定义排序）
    *
    *
    * 第一步 拿到通过过滤的session,访问过的所有品类
    * 第二步 计算出session访问过的所有品类、下单、支付次数，可能和第一步计算出来的品类进行join
    * 第三步 自定义排序,根据点击、下单、支付
    * 将点击、下单、支付次数 作为二次排序的key,使用sortBykey(false)，按照自定义key,进行降序排序
    * 使用take(n),拿到前n排序
    *
    * 第四步 写入mysql.
    *
    * 本地测试
    *
    * */

    val categorySortKeys = getTopNCateGory(res, sessionInfo)



    /**
      *
      * 热门商品topn session
      *
      *
      */

    val sessionDetail = res.join(sessionInfo).map(t => (t._1, t._2._2))
    getTop10Session(sc,sessionDetail,categorySortKeys)


    sc.stop()

  }

  /**
    * 模拟hive仓库中数据，本地开发环境测试用
    *
    * @param sc
    * @param sqlContext
    */
  def mockData(sc: SparkContext, sqlContext: SQLContext) {
    if (Constants.LOCAL) {
      LocalDataGenerator.mockData(sc, sqlContext)
    }
  }


  def getActionRDDByDataRange(sqlContext: SQLContext, taskParam: JSONObject) = {

    //    val endDate   = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    //    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)


    val sql = "select" +
      " *" +
      " from " +
      " user_visit_action "
    //        " where  date>= '" +startDate + "' "+
    //        " date <= '" +endDate +"' "
    val dataFrame = sqlContext.sql(sql)
    dataFrame
  }



}
