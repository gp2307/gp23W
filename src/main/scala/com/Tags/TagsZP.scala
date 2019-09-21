package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsZP extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val provincename = row.getAs[String]("provincename")
    if(StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    list
  }
}
