package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
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


//    //调用hbaseAPI
//    val load = ConfigFactory.load()
//    val HbaseTableName = load.getString("HBASE.tableName")
//    //创建hadoop任务
//    val configuration = session.sparkContext.hadoopConfiguration
//    //配置hbase连接
//    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
//    //获取连接
//    val connection = ConnectionFactory.createConnection(configuration)
//    val admin = connection.getAdmin
//    //判断表是否已使用
//    if(!admin.tableExists(TableName.valueOf(HbaseTableName))){
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
//      val columnDescriptor = new HColumnDescriptor("tags")
//      tableDescriptor.addFamily(columnDescriptor)
//      admin.createTable(tableDescriptor)
//      admin.close()
//      connection.close()
//    }
//    val conf = new JobConf(configuration)
//    //指定输出类型
//    conf.setOutputFormat(classOf[TableOutputFormat])
//    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)


    //读取文件
    val frame = session.read.parquet(inputPath)
    //处理数据信息
    val res = frame.rdd.map(row=>{
      //获取用户的唯一ID
      val userId = TagUtils.getOneUserId(row)
//      val userId = TagUtils.getallUserId(row).mkString(",").hashCode.toLong
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
      (userId,(adList:::appList:::adplList:::osList:::netList:::ispList:::keywordlist:::provinceList:::cityList:::bussinessList))
    }).reduceByKey((x,y)=>{
      x:::y
    }).mapValues(x=>{
      val list1 = x.groupBy(_._1).mapValues(x=>x.map(x=>x._2).reduce(_+_)).toList
      list1
    })
//    res.map{
//      case (userId,userTags) =>{
//        // 设置rowkey和列、列名
//        val put = new Put(Bytes.toBytes(userId))
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("20190922"),Bytes.toBytes(userTags.mkString(",")))
//        (new ImmutableBytesWritable(),put)
//      }
//    }.saveAsHadoopDataset(conf)
    res.foreach(println)
    session.stop()
  }
}
