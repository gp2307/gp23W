package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object JsonExam {
  def main(args: Array[String]): Unit= {
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName("json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val src =sc.textFile(inputPath)
    val jsonparse = src.map(t => {
      var list = List[(String,Int)]()
      val jsonparse = JSON.parseObject(t)
      if (jsonparse.getIntValue("status") == 1) {
        val regeocode = jsonparse.getJSONObject("regeocode")
        if (regeocode != null && !regeocode.keySet().isEmpty) {
          val poisArray = regeocode.getJSONArray("pois")
          if (poisArray != null && !poisArray.isEmpty) {
            for (item <- poisArray.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val json = item.asInstanceOf[JSONObject]
                list:+=(json.getString("businessarea"),1)
              }
            }
          }
        }
      }
      list
    })
    val res: RDD[(String,Int)] = jsonparse.flatMap(x=>x).reduceByKey(_+_)
    res.foreach(println)
  }
}