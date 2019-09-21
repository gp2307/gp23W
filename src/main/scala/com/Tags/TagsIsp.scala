package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 设备运营商标签
  */
object TagsIsp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val isp = row.getAs[String]("ispname")
    if(StringUtils.isNotBlank(isp)){
      isp match {
        case t if (t.equals("移动"))  => list:+=("D00030001",1)
        case t if (t.equals("联通"))  => list:+=("D00030002",1)
        case t if (t.equals("电信"))  => list:+=("D00030003",1)
        case _ => list:+=("D00030004",1)
      }
    }
    list
  }
}
