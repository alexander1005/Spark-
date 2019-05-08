package com.ltz.spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.AccumulatorParam

/**
  *
  */


object SessionAggrStatAccumular extends AccumulatorParam[String] {

  def getFieldFromConcatString(v1: String, v2: String, v3: String): String = {
    com.ltz.spark.util.StringUtils.getFieldFromConcatString(v1, v2, v3);
  }
  def setFieldInConcatString(v1: String, v2: String, v3: String,v4:String):String={
    com.ltz.spark.util.StringUtils.setFieldInConcatString(v1, v2, v3,v4);
  }


  override def addInPlace(v1: String, v2: String): String = add(v1, v2)

  /**
    * 数据初始化
    *
    * @param initialValue
    * @return
    */
  override def zero(initialValue: String): String = Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"


  override def addAccumulator(v1: String, v2: String): String = add(v1, v2)

  /**
    * 连接串
    *
    * @param v1
    * @param v2 范围区间
    * @return 更新以后的连接串
    */


  def add(v1: String, v2: String): String = {
    if (StringUtils.isBlank(v1)) {
      v2
    } else {
      val oldValue = getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue!=null){
        val newValue = String.valueOf(oldValue.toInt+1)
        setFieldInConcatString(v1,"\\|",v2,newValue)
      }else{
        v1
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(zero(""))
  }
}

