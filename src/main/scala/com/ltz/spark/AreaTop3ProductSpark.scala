package com.ltz.spark

import com.ltz.spark.UserVisitSessionAnaly.{ mockData, sqlcontext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AreaTop3ProductSpark {


  def generateTempAreaProductClickCountTable(sqlContext: SQLContext) = {
     sqlContext.sql("select" +
       " area,product_id,count(*) click_count,group_concat_distinct(concat_long_string(cityid,cityname,':'))  city_infos," +
       " CASE WHEN area='华北' or area='华东' then 'A级' "+
       "  WHEN area='华南' or area='华中' then 'B级' "+
       "  WHEN area='西北' or area='西 东' then 'C级' "+
       "  ELSE 'D级' "+
       " END area_level"+
       " from " +
       " tmp_clk_prod_basic" +
       " group by area,product_id").registerTempTable("tmp_area_product_click_count")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("visit").setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.storage.memoryFraction", "0.4")
    val sc = new SparkContext(conf)
    val sqlContext = sqlcontext(sc)
    //生成模拟数据
    mockData(sc, sqlContext)
    //注册自定义函数
    sqlContext.udf.register("concat_long_string",Concat2UDF,DataTypes.StringType);

    //获取mysql taskid
    val taskId = 3

    val actionRdd = getUserClickActionByDate(null, null, sqlContext)

    val cityInfoRdd = cityInfoAction(sqlContext)

    val info = clickActionJoinCityInfo(actionRdd, cityInfoRdd)
    //生成点击商品基础信息临时表
    val stuctFields = mutable.ArrayBuffer[StructField]()
    stuctFields+=DataTypes.createStructField("city_id",DataTypes.LongType,true)
    stuctFields+=DataTypes.createStructField("city_name",DataTypes.StringType,true)
    stuctFields+=DataTypes.createStructField("area",DataTypes.StringType,true)
    stuctFields+=DataTypes.createStructField("product_id",DataTypes.LongType,true)
    val schema = DataTypes.createStructType(stuctFields.toArray)
    //注册成临时表
    val infoframe = sqlContext.createDataFrame(info,schema)
    infoframe.registerTempTable("tmp_clk_prod_basic")
    //注册udf
    sqlContext.udf.register("concat_long_string", Concat2UDF,DataTypes.StringType)
    sqlContext.udf.register("group_concat_distinct", GroupConcatDistinctUDAF)
    //生成临时表 和区域 各点击商品次数
    generateTempAreaProductClickCountTable(sqlContext)
    sc.stop()

  }

  def cityInfoAction(sqlContext: SQLContext) = {

    val dataFrame = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://127.0.0.1:3306/sparksession?user=root&password=root",
      "dbtable" -> "city")).load()
    dataFrame.rdd.map(t => (t.getLong(0), t))
  }

  def getUserClickActionByDate(startDate: String, endDate: String, sqlContext: SQLContext) = {

    val sql =
      "select " +
        "city_id,click_product_id as " +
        " product_id " +
        "from user_visit_action" +
        " where" +
        " click_product_id is not null " +
        "and click_product_id !='NULL' " +
        "and click_product_id !='null'  and click_product_id !='Null' "

    val dataFrame = sqlContext.sql(sql)
    dataFrame.rdd.map(t => (t.getLong(1), t))
  }

  def clickActionJoinCityInfo(actionRdd: RDD[(Long, Row)], cityInfoRdd: RDD[(Long, Row)]) = {
    actionRdd.join(cityInfoRdd).map(t => {
      //城市id
      val cityId = t._1.toLong
      //产品id
      val productId = t._2._1.getLong(1)
      val cityName = t._2._2.getString(1)
      val area = t._2._2.getString(2)
      Row(cityId,cityName,area,productId)
    })


  }


}
