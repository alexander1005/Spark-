package com.ltz.spark

import org.apache.spark.sql.api.java.UDF3

object Concat2UDF extends UDF3[Long,String,String,String]
{
  override def call(t1: Long, t2: String, t3: String): String = t1+t3+t2
}
