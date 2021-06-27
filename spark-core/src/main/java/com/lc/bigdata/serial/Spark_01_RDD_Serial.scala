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

/**
 * Java 的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也
 * 比较大。Spark 出于性能的考虑，Spark2.0 开始支持另外一种 Kryo 序列化机制。Kryo 速度
 * 是 Serializable 的 10 倍。当 RDD 在 Shuffle 数据的时候，简单数据类型、数组和字符串类型
 * 已经在 Spark 内部使用 Kryo 来序列化。
 * 注意：即使使用 Kryo 序列化，也要继承 Serializable 接口。
 *
 * @param query
 */

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