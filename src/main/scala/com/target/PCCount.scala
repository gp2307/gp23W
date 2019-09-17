package com.target

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PCCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 2 ){
      println("目录不正确，退出程序")
      sys.exit()
    }
    val sparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()
    val  Array(inputPath,outputPath) = args
    val frame: DataFrame = sparkSession.read.parquet(inputPath)
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val res = frame.groupBy($"provincename",$"cityname").agg(count($"*") as "ct").select($"ct",$"provincename",$"cityname")
    res.cache()
//    res.write.partitionBy("provincename","cityname").json(outputPath)
    val load = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    res.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)
    sparkSession.stop()
  }
}
