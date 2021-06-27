package com.lc.bigdata.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-27
 * @time 9:14
 */
object Spark_01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    //获取与spark的连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("serial")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")


//    search.getMatch1(rdd).collect().foreach(println)


    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }


}


//查询对象
//类的构造器其实是类的属性，构造参数需要进行闭包检测，其实就等同于类进行闭包检测
class Search(query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 函数序列化案例
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val s = query
    rdd.filter(x => x.contains(s))
  }
}