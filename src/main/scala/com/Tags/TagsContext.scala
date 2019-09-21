package com.Tags

import com.util.TagUtils
import org.apache.spark.sql.SparkSession

/**
  * 标签的上下文主类
  */
object TagsContext {
  case class TagClass(userId:String,adTag:List[(String,Int)],appTag:List[(String,Int)],bussinessTag:List[(String,Int)])
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath) = args
    val session = SparkSession.builder().appName("tags").master("local").getOrCreate()
    import session.implicits._
    val frame = session.read.parquet(inputPath)
    //处理数据信息
    frame.rdd.map(row=>{
      //获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      //接下来标签实现
      //广告位类型
      val adList = TagsAd.makeTags(row)
      //App 名称
       val appList: List[(String, Int)] = TagsApp.makeTags(row)

      val adplList = TagsAdpl.makeTags(row)
      val osList = TagsOS.makeTags(row)
      val netList = TagsNet.makeTags(row)
      val ispList = TagsIsp.makeTags(row)
      val bussinessList = BusinessTag.makeTags(row)
      val keywordlist = TagsKw.makeTags(row)
      val provinceList = TagsZP.makeTags(row)
      val cityList = TagsZC.makeTags(row)
      (userId,(adList,appList,adplList,osList,netList,ispList,keywordlist,provinceList,cityList,bussinessList))
    }).reduceByKey((x,y)=>{
      val list1:List[(String,Int)] = (x._1:::y._1)
      val list2:List[(String,Int)] = (x._2:::y._2)
      val list3:List[(String,Int)] = (x._3:::y._3)
      val list4:List[(String,Int)] = (x._4:::y._4)
      val list5:List[(String,Int)] = (x._5:::y._5)
      val list6:List[(String,Int)] = (x._6:::y._6)
      val list7:List[(String,Int)] = (x._7:::y._7)
      val list8:List[(String,Int)] = (x._8:::y._8)
      val list9:List[(String,Int)] = (x._9:::y._9)
      val list10:List[(String,Int)] = (x._10:::y._10)
      (list1,list2,list3,list4,list5,list6,list7,list8,list9,list10)
    }).mapValues(x=>{
      val list1:List[(String,Int)] = (x._1).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list2:List[(String,Int)] = (x._2).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list3:List[(String,Int)] = (x._3).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list4:List[(String,Int)] = (x._4).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list5:List[(String,Int)] = (x._5).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list6:List[(String,Int)] = (x._6).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list7:List[(String,Int)] = (x._7).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list8:List[(String,Int)] = (x._8).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list9:List[(String,Int)] = (x._9).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      val list10:List[(String,Int)] = (x._10).groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      (list1,list2,list3,list4,list5,list6,list7,list8,list9,list10)
    }).foreach(println)
  }
}
