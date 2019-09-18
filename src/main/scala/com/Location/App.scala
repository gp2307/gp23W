package com.Location


import java.util.{Date, Random}

import com.util.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession


class App{

}
object App {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath,outputPath,doc)=args
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("322")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据字典
    val docmap = sparkSession.sparkContext.textFile(doc).map(_.split("\\s",-1)).filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()
    //进行广播
    val broadcast = sparkSession.sparkContext.broadcast(docmap)
    //读取数据文件
    val frame = sparkSession.read.parquet(inputPath)
    frame.rdd.map(row=> {
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"其他")
      }
      // 根据指标的字段获取数据
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //处理请求数
      val reqpt = RptUtils.ReqPt(requestmode, processnode)
      //处理点击数
      val chickpt = RptUtils.chickPt(requestmode, iseffective)

      val adpt = RptUtils.adPt(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val allList: List[Double] = reqpt ++ List(adpt(0), adpt(1)) ++ chickpt ++ List(adpt(2), adpt(3))
      (appName, allList)
    }
    ).reduceByKey((list1:List[Double],list2:List[Double])=>{
      list1.zip(list2).map(x=>{
        x._1+x._2
      })
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath+new Date().getTime.toString)
  }
}
