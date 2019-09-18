package com.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object LabelUtils {
  //1)广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，小于 10 补 0，把广告位类型名称，LN 插屏->1
  //,concat(when(col("adspacetype")<10,"LC0"),col("adspacetype"),when(col("adspacetype")===col("adspacetype"),"->1"))
  def adLabel(frame:DataFrame):DataFrame={
    frame.select(col("*"),struct(concat(when(col("adspacetype")<10,"LC 0") when(col("adspacetype")>10,"LC "),col("adspacetype")),when(col("adspacetype")===col("adspacetype"),1)) as "adlabel",
      struct(concat(when(col("adspacetypename")===col("adspacetypename"),"LN "),col("adspacetypename")),when(col("adspacetypename")===col("adspacetypename"),1)) as "adnamelabel")
  }
//  2)App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
def appLabel(frame:DataFrame):DataFrame={
  frame.select(col("*"),struct(concat(when(col("appname")===col("appname"),"APP "),col("appname")),when(col("appname")===col("appname"),1)) as "applabel")
}
//  3)渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
def cnLabel(frame:DataFrame):DataFrame={
  frame.select(col("*"),struct(concat(when(col("adplatformproviderid")===col("adplatformproviderid"),"CN "),col("adplatformproviderid")),when(col("adplatformproviderid")===col("adplatformproviderid"),1)) as "cnlabel")
}
//  4)设备：
//    a)(操作系统 -> 1)
//    b)(联网方 -> 1)
//    c)(运营商 -> 1)
//  5)关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
//  超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
//  6)地域标签（省标签格式：ZPxxx->1, 地市标签格式: ZCxxx->1）xxx 为省或市名称
//  7)商圈标签
//  8)上下文标签： 读取日志文件，将数据打上上述 6 类标签，并根据用户 ID 进行当前文件的合并，数据保存格式为：userId	K 青云志:3 D00030002:1 ……
}