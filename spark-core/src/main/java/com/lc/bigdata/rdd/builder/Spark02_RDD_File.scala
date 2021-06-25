package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建rdd
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //path的路径默认是以当前环境的根路径作为基准，可以写绝对路径，也可以写相对路径
//    val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //path可以是文件的具体路径，也可以是目录名称
//    val rdd: RDD[String] = sc.textFile("datas")

    //path还可以使用通配符*
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")

    //path还可以是分布式存储系统路径：HDFS
//    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)

    //关闭连接
    sc.stop()




  }

}
