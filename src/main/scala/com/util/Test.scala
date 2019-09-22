package com.util

import com.Tags.BusinessTag
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("test").getOrCreate()
    var rdd = session.read.parquet("F:\\data\\parquet").rdd
    rdd.map(row=>{
      val bussinessList = BusinessTag.makeTags(row)
      bussinessList.sortBy(_._1)
    }).sortBy(_.length).foreach(println)
    session.stop()
  }
}
