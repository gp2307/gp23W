package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object TagsAdpl extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]

    val adpl = row.getAs[Int]("adplatformproviderid")
    if(adpl != null){
      list:+=("CN"+adpl,1)
    }
    list
  }
}
