package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //从文件中创建RDD，将文件中的数据作为处理的数据源

    //textFile:以行为单位来读取数据
    //  读取的数据都是字符串
    //wholeTextFiles:以文件为单位来读取数据
    //  读取的结果表示为：元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")

    rdd.collect().foreach(println)

    //关闭连接
    sc.stop()


  }

}
