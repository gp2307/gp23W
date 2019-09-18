package com.label

import com.util.LabelUtils
import org.apache.spark.sql.SparkSession

object AdLabel {
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
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val frame1 = LabelUtils.cnLabel(LabelUtils.appLabel(LabelUtils.adLabel(frame)))
    frame1.show()
    sparkSession.stop()
  }
}
