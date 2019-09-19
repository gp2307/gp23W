package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {
  def getBusinessFormAmap(long:Double,lat:Double):String={
//    https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=0392f8c0d3862f699d5c16aceb5e1cf1&radius=1000&extensions=all
    val location = long+","+lat
    //获取url
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=0392f8c0d3862f699d5c16aceb5e1cf1&radius=1000&extensions=all"
    //调用http接口发送请求
    val jsonstr = HttpUtil.get(url)
    //解析json
    val jssonObject1: JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val status = jssonObject1.getIntValue("status")
    if(status == 0) return  ""
    //如果不为空
    val jssonObject2 = jssonObject1.getJSONObject("regeocode")
    if(jssonObject2 == null) return ""
    val jsonObject3: JSONObject = jssonObject2.getJSONObject("addressComponent")
    if(jsonObject3 == null) return ""
    val jsonArray = jsonObject3.getJSONArray("businessAreas")
    if(jsonArray == null) return ""
    //定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    //循环数组
    for(item <- jsonArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    //商圈名字
    result.mkString(",")
  }
}
