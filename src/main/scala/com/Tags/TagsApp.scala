package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsApp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val appname = row.getAs[String]("appname")

    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }
    list
  }
}
