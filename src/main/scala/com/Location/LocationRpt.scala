package com.Location

import com.util.RptUtils
import org.apache.spark.sql.SparkSession

/**
  * 统计地域指标
  */
object LocationRpt {
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
    //获取数据
    val frame = sparkSession.read.parquet(inputPath)
    frame.rdd.map(row=>{
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
      val reqpt = RptUtils.ReqPt(requestmode,processnode)
      //处理点击数
      val chickpt = RptUtils.chickPt(requestmode,iseffective)

      val adpt = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      val allList:List[Double] = reqpt ++ List(adpt(0),adpt(1)) ++ chickpt ++ List(adpt(2),adpt(3))
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1:List[Double],list2:List[Double])=>{
      list1.zip(list2).map(x=>{
        x._1+x._2
      })
    }).map(t=>t._1+","+t._2.mkString(","))
      .foreach(println)
  }
}
