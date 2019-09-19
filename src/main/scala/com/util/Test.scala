package com.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    val arr =Array("https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957&key=0392f8c0d3862f699d5c16aceb5e1cf1&radius=1000&extensions=all")
    var rdd = session.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HttpUtil.get(t)
    }).foreach(println)
    session.stop()
  }
}
