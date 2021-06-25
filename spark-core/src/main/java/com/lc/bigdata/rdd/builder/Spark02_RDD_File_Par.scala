package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //textFile将文件作为数据处理的数据源，默认可以设定分区
    //  minPartitions:最小分区数量
    //  math.min(defaultParallelism, 2)
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //如果不想使用默认的分区数量，可以通过第二个参数指定分区数量

    //spark底层读取文件其实就是hadoop的读取方式
    //分区数量的读取方式:
    //  totalSize = 7
    //  goalSize = 7 / 2 = 3(byte)
    //7 / 3 = 2 ......1

    val rdd: RDD[String] = sc.textFile("datas/1.txt", 3)

    rdd.saveAsTextFile("output")



    //关闭连接
    sc.stop()


  }

}
