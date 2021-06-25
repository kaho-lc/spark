package com.lc.bigdata.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark02_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //如果数据源为多个文件，那么计算分区时以文件为单位进行分区

    //关闭连接
    sc.stop()


  }

}
