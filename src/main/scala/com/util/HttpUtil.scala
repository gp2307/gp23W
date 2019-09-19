package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Http请求协议 GET请求
  */
object HttpUtil {
  /**
    * GET请求
    * @param url
    * @return
    */
  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpget: HttpGet = new HttpGet(url)
    //获取发送请求
    val response: CloseableHttpResponse = client.execute(httpget)
    //处理返回请求结果
    //解决乱码
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
