package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //数据分区的分配
    //1.数据以行为单位进行读取
    //  spark读取文件采用的是hadoop的方式读取，和字节数没有关系
    //2.数据读取时是以偏移量为单位的

    //3.数据分区的偏移量范围
    //




    //关闭连接
    sc.stop()


  }

}
