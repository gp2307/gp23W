package com.Tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, String2Type, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    if(String2Type.toDouble(row.getAs[String]("long")) >=73
      && String2Type.toDouble(row.getAs[String]("long")) <=136
      && String2Type.toDouble(row.getAs[String]("lat"))>=3
      && String2Type.toDouble(row.getAs[String]("lat"))<=53){
      // 经纬度
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      // 获取到商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNotBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list :+= (str,1)
        })
      }
    }
    list
  }
  /**
    *获取商圈信息
   */
  def getBusiness(long:Double,lat:Double):String={
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

    var business = redis_queryBusiness(geohash)
    if(business == null){
      business = AmapUtil.getBusinessFormAmap(long,lat)
      if(business != null){
        redis_insertBusiness(geohash,business)
      }
    }
    business
  }
  def redis_queryBusiness(geohash: String) = {
    val jedis = JedisConnectionPool.getConnect()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }
  def redis_insertBusiness(geoHash:String,business:String)={
    val jedis = JedisConnectionPool.getConnect()
    jedis.set(geoHash,business)
    jedis.close()
  }
}
