package com.ltz.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}


/**
  * 组内拼接 去重函数
  */
object GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

  //指定输入数据的字段与类型
  override def inputSchema: StructType =DataTypes.createStructType(Array[StructField](
    DataTypes.createStructField("cityInfo",DataTypes.StringType,true)
  ))
  //指定缓存数据字段与类型
  override def bufferSchema: StructType = DataTypes.createStructType(Array[StructField](
    DataTypes.createStructField("bufferCityInfo",DataTypes.StringType,true)
  ))
  //指定是否确定性
  override def deterministic: Boolean = true
  //指定返回字段类型
  override def dataType: DataType = DataTypes.StringType

  /**
    *初始化，指定初始值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0,"")

  /**
    *可以认为是，一个个将组内的字段值传递进来，实现拼接逻辑
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)
    if (!bufferCityInfo.contains(cityInfo)){
      if (""==bufferCityInfo)
        bufferCityInfo = (bufferCityInfo + cityInfo)
      else
        bufferCityInfo = (","+cityInfo)
    }
    buffer.update(0,bufferCityInfo)

  }

  /**
    * 合并 update操作，可能是针对一个分组内部分数据，在某个节点上发生的，但是可能
    * 一个分组内的数据，会分布在多个节点上处理，此时要用此方法将各个节点上分布拼接好的串合并起来
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var cityInfo1 = buffer1.getString(0)
    val cityInfo2 = buffer2.getString(0)
    for (info <- cityInfo2.split(",")){
      if (!cityInfo1.contains(info)){
        if (""==cityInfo1)
          cityInfo1 = (cityInfo1 + info)
        else
          cityInfo1 = (","+info)
      }
    }
    buffer1.update(0,cityInfo1)
  }

  override def evaluate(buffer: Row): String = buffer.getString(0)
}
