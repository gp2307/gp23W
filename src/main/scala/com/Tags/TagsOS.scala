package com.Tags

import com.util.Tag
import org.apache.spark.sql.Row

/**
  * 操作系统标签
  */
object TagsOS extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val os = row.getAs[Int]("client")
    if(os != null){
      os match {
        case t if (t == 1)  => list:+=("D00010001",1)
        case t if (t == 2)  => list:+=("D00010002",1)
        case t if (t == 3)  => list:+=("D00010003",1)
        case _ => list:+=("D00010004",1)
      }
    }
    list
  }
}
