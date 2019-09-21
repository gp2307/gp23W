package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsZC extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val cityname = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(cityname)){
      list:+=("APP"+cityname,1)
    }
    list
  }
}
