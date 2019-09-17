package com.target

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Target321C {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val sparkSession = SparkSession.builder()
      .appName("core")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val rdd: RDD[Row] = sparkSession.read.parquet(inputPath).rdd
    val rdd1 = rdd.map(r => {
      //      (r.getString(24),r.getString(25),r.getInt(8),r.getInt(35),r.getInt(30),r.getInt(31),r.getInt(39),r.getInt(42),r.getInt(2),r.getDouble(40))
      val c1: Int = if (r.getInt(8) == 1 && r.getInt(35) >= 1) 1 else 0
      val c2: Int = if (r.getInt(8) == 1 && r.getInt(35) >= 2) 1 else 0
      val c3: Int = if (r.getInt(8) == 1 && r.getInt(35) >= 3) 1 else 0
      val c4: Int = if (r.getInt(30) == 1 && r.getInt(31) == 3 && r.getInt(39) == 1) 1 else 0
      val c5: Int = if (r.getInt(30) == 1 && r.getInt(31) == 3 && r.getInt(42) == 1 && r.getInt(2) != 0) 1 else 0
      val c6: Int = if (r.getInt(8) == 2 && r.getInt(30) == 1) 1 else 0
      val c7: Int = if (r.getInt(8) == 3 && r.getInt(30) == 1) 1 else 0
      val c8: Int = if (r.getInt(30) == 1 && r.getInt(31) == 3 && r.getInt(42) == 1) 1 else 0
      val c9: Int = if (r.getInt(30) == 1 && r.getInt(31) == 3 && r.getInt(42) == 1) 1 else 0
      val s1 = r.getDouble(40)
      ((r.getString(24), r.getString(25)), Nil :+ c1.toDouble :+ c2.toDouble :+ c3.toDouble :+ c4.toDouble :+ c5.toDouble :+ c6.toDouble :+ c7.toDouble :+ c8.toDouble :+ c9.toDouble :+ s1)
    })
    rdd1.cache()
    val rdd2 = rdd1.reduceByKey((x:List[Double],y:List[Double])=>{
      Nil:+(x(0)+y(0)):+(x(1)+y(1)):+(x(2)+y(2)):+(x(3)+y(3)):+(x(4)+y(4)):+(x(5)+y(5)):+(x(6)+y(6)):+(x(7)+y(7)):+(x(8)+y(8)):+(x(9)+y(9))
    })
    val res1 = rdd2.mapValues(x => {
      (x(0), x(1), x(2), x(3), x(4), x(4) / x(3), x(5), x(6), x(6) / x(5), x(9) / x(7) / 1000, x(9) / x(8) / 1000)
    })
    val res2 = rdd1.map(x=>((x._1._1,"all"),x._2)).reduceByKey((x:List[Double],y:List[Double])=>{
      Nil:+(x(0)+y(0)):+(x(1)+y(1)):+(x(2)+y(2)):+(x(3)+y(3)):+(x(4)+y(4)):+(x(5)+y(5)):+(x(6)+y(6)):+(x(7)+y(7)):+(x(8)+y(8)):+(x(9)+y(9))
    }).mapValues(x => {
      (x(0), x(1), x(2), x(3), x(4), x(4) / x(3), x(5), x(6), x(6) / x(5), x(9) / x(7) / 1000, x(9) / x(8) / 1000)
    })
    res1.union(res2).sortByKey().foreach(println)

    sparkSession.stop()
  }
}
