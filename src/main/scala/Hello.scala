import org.apache.spark.sql.SparkSession

object Hello {
  def main(args: Array[String]): Unit = {
    println("hello word")
    val sparkSession = SparkSession.builder().master("local[2]").appName("one").getOrCreate()
    val df = sparkSession.read.parquet("F:\\2.Hadoop\\上课视频\\数仓项目阶段\\第七周\\release_session\\bdp_day=20190613\\15603638400003h4gka4h")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    df.show()
    sparkSession.stop()
  }
}
