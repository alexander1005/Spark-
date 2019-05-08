package com.ltz.spark


import scala.reflect.io.File

object timer {


  def main(args: Array[String]): Unit = {

      val f = File("/tmp/lineitem5.csv")
//      while (!f.exists){}
//      val start = System.currentTimeMillis();
//      while (f.exists){}
//      println(s"use time ${ (System.currentTimeMillis() -start)/1000} s ")
      val lines=  5432671
      val size = 5432671/20
      val read =f.bufferedReader()
      read.readLine()
      for(i <- 0 to 19){
        val f = File(s"/tmp/sss/lineitem5${i}.csv")
        val writer = f.bufferedWriter(true)
        for(j <-0 to size){
          writer.write("l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,l11,l12,l13,l14,l15,l16\n")
          val str = read.readLine() + "\n"
          var flag = true
          val strings = str.split(",")
          strings.foreach(row =>{
            if (strings.length!=16)
              flag =false
            for(i <-strings){
              if (i!=null && i.length>0 && strings.length==16){
              }else{
                flag =false
                println("排除数据")
                println(str)
              }
            }
          })
          if (flag)
            writer.write(str)
        }
      }
  }
}
