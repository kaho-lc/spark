package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //自定义分区数
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4 , 5)  , 3
    )

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //关闭连接
    sc.stop()
  }
}
