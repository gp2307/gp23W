package com.Tags

import com.util.TagUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 标签的上下文主类
  */
object TagsContext2 {
  case class TagClass(userId:String,adTag:List[(String,Int)],appTag:List[(String,Int)],bussinessTag:List[(String,Int)])
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath) = args
    val session = SparkSession.builder().appName("tags").master("local").getOrCreate()



    //读取文件
    val frame = session.read.parquet(inputPath)
    //处理数据信息
    val allUserId = frame.rdd.map(row=>{
      //获取用户的唯一ID
      val userId = TagUtils.getallUserId(row)
      (userId,row)
    })
//    构建点集合
    val vertexRDD = allUserId.flatMap(rows => {
      //获取所有数据
      val row = rows._2

      //接下来标签实现
      val adList = TagsAd.makeTags(row)
      val appList: List[(String, Int)] = TagsApp.makeTags(row)
      val adplList = TagsAdpl.makeTags(row)
      val osList = TagsOS.makeTags(row)
      val netList = TagsNet.makeTags(row)
      val ispList = TagsIsp.makeTags(row)
      val bussinessList = BusinessTag.makeTags(row)
      val keywordlist = TagsKw.makeTags(row)
      val provinceList = TagsZP.makeTags(row)
      val cityList = TagsZC.makeTags(row)
      val tagList = adList ++ appList ++ adplList ++ osList ++ netList ++ ispList ++ bussinessList ++ keywordlist ++ provinceList ++ cityList
      //保留用户ID
      val VD = rows._1.map((_, 0)) ++ tagList
      //思考  1、如何保证其中一个ID携带着用户的标签
      //     2、用户ID的字符串如何处理
      rows._1.map(uId => {
        if (rows._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    //构造边集合
    val edgeRDD = allUserId.flatMap(row=>{
        row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //构造图
    val graph = Graph(vertexRDD,edgeRDD)
//    根据图计算中的连通图算法，通过图中的分支，连通所有的点
//    然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    vertices.join(vertexRDD).map{
      case (uId,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    }).foreach(println)
    session.stop()
  }
}
