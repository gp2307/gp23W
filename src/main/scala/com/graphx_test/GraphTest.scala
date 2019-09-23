package com.graphx_test

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例（好友关联推荐）
  */
object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()
    //创建点和边
    //构建点的集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L, ("小红", 26)),
      (2L, ("小黄", 26)),
      (6L, ("小蓝", 26)),
      (9L, ("小绿", 26)),
      (133L, ("小青", 26)),
      (138L, ("小紫", 26)),
      (158L, ("小黑", 26)),
      (16L, ("小白", 26)),
      (44L, ("小棕", 26)),
      (21L, ("小橙", 26)),
      (5L, ("小棕", 26)),
      (7L, ("小靛", 26))
    ))

    //构建边集合
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0),
      Edge(16L, 138L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0)
    ))

    //构建图
    val graph = Graph(vertexRDD,edgeRDD)
    //取顶点
    val vertices = graph.connectedComponents().vertices
    vertices.foreach(println)
    //匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_++_)
      .foreach(println)
    spark.stop()
  }
}
