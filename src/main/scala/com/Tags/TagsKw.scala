package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsKw extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val keywords = row.getAs[String]("keywords")
    if(StringUtils.isNotBlank(keywords)){
      val keylist = keywords.split("\\|").map(x=>("K"+x,1)).toList
      return keylist
    }
    list
  }
}
