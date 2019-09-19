package com.Tags

import com.util.TagUtils
import org.apache.spark.sql.SparkSession

/**
  * 标签的上下文主类
  */
object TagsContext {
  case class TagClass(userId:String,adList:List[(String,Int)],bussinessLisr:List[(String,Int)])
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
    frame.map(row=>{
      //获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
      //接下来标签实现
      val adList = TagsAd.makeTags(row)
      val bussinessLisr = BusinessTag.makeTags(row)
      TagClass(userId,adList,bussinessLisr)
    }).show()
  }
}
