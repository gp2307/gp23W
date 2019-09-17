package com.Location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProCityCt {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath)=args
    val session = SparkSession.builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val frame = session.read.parquet(inputPath)
    //注册临时视图
    frame.createTempView("log")
    val frame2 = session.sql(
      """
select provincename,cityname,count(*) as ct
from log
group by provincename,cityname
      """.stripMargin)
    //存MySQL
    //通过config配置文件依赖加载相关的配置信息
    val load = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    frame2.write.mode(SaveMode.Overwrite)
      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)
    session.stop()
  }
}
