package com.target

import org.apache.spark.sql.SparkSession

object Target322 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("322")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val frame = sparkSession.read.parquet(inputPath)
    frame.cache()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val res1 = frame.select($"ispname" as "key",$"winprice",
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
    val res2 = frame.select($"networkmannername" as "key",$"winprice",
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
    val res3 = frame.select(
      when($"client"===1,"手机")
        when($"client"===2,"平板")
        when($"client">2,"其他") as "key",
      $"winprice",
      when($"requestmode" === 1 and $"PROCESSNODE">=1,1) as "c1",
      when($"requestmode" === 1 and $"PROCESSNODE">=2,1) as "c2",
      when($"requestmode" === 1 and $"PROCESSNODE"===3,1) as "c3",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISBID"===1,1) as "c4",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1 and $"adorderid".=!=(0),1) as "c5",
      when($"REQUESTMODE"===2 and $"ISEFFECTIVE"===1,1) as "c6",
      when($"REQUESTMODE"===3 and $"ISEFFECTIVE"===1,1) as "c7",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1,1) as "c8",
      when($"ISEFFECTIVE"===1 and $"ISBILLING"===1 and $"ISWIN"===1,1) as "c9"
    ).groupBy($"key")
      .agg(
        count($"c1") as "c1",
        count($"c2") as "c2",
        count($"c3") as "c3",
        count($"c4") as "c4",
        count($"c5") as "c5",
        count($"c5")/count($"c4") as "s1",
        count($"c6") as "c6",
        count($"c7") as "c7",
        count($"c7")/count($"c6") as "s2",
        sum(when($"c8"===1,$"winprice"))/1000 as "s3",
        sum(when($"c9"===1,$"winprice"))/1000 as "s4"
      )
    val res4 = frame.select(
      when($"client"===1,"android")
      when($"client"===2,"ios")
      when($"client">2,"其他") as "key",
      $"winprice",
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
    val res5 = frame.select($"appname" as "key",$"winprice",
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
    val res6 = frame.select($"adplatformkey" as "key",$"winprice",
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
    val res = res1.union(res2).union(res3).union(res4).union(res5).union(res6)
    res.show(100)
    sparkSession.stop()
  }
}
