package com.target

import org.apache.spark.sql.SparkSession

object Target321 {
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("参数输入错误")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("target321")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val frame = sparkSession.read.parquet(inputPath)
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val res = frame.select(concat_ws(",",$"provincename",$"cityname") as "key",$"winprice",
      when($"requestmode" === 1 and $"PROCESSNODE">=1,1) as "c1",
      when($"requestmode" === 1 and $"PROCESSNODE">=2,1) as "c2",
      when($"requestmode" === 1 and $"PROCESSNODE"===3,1) as "c3",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISBID"===1,1) as "c4",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1 and $"adorderid".=!=(0),1) as "c5",
      when($"REQUESTMODE"===2 and $"ISEFFECTIVE"===1,1) as "c6",
      when($"REQUESTMODE"===3 and $"ISEFFECTIVE"===1,1) as "c7",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1,1) as "c8",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1,1) as "c9"
    ).groupBy($"key").agg(count($"c1") as "c1",count($"c2") as "c2",count($"c3") as "c3",count($"c4") as "c4",count($"c5") as "c5",count($"c5")/count($"c4") as "s1",count($"c6") as "c6",count($"c7") as "c7",count($"c7")/count($"c6") as "s2",sum(when($"c8"===1,$"winprice"))/1000 as "s3",sum(when($"c9"===1,$"winprice"))/1000 as "s4")
    res.cache()
    val res1 = res.groupBy(split($"key",",")(0) as "key").agg(sum($"c1") as "c1",sum($"c2") as "c2",sum($"c3") as "c3",sum($"c4") as "c4",sum($"c5") as "c5",sum($"s1") as "s1",sum($"c6") as "c6",sum($"c7") as "c7",sum($"s2") as "s2",sum($"s3") as "s3",sum($"s4") as "s4")
    res.union(res1).sort($"key").show()
    sparkSession.stop()
  }
}
