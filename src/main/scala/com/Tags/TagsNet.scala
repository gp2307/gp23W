package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 设备联网方式标签
  */
object TagsNet extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val net = row.getAs[String]("networkmannername")
    if(StringUtils.isNotBlank(net)){
      net match {
        case t if (t.equals("WIFI"))  => list:+=("D00020001",1)
        case t if (t.equals("4G"))  => list:+=("D00020002",1)
        case t if (t.equals("3G"))  => list:+=("D00020003",1)
        case t if (t.equals("2G"))  => list:+=("D00020004",1)
        case _ => list:+=("D00020005",1)
      }
    }
    list
  }
}
