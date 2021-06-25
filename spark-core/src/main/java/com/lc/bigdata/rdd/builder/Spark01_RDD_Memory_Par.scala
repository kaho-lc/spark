package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //自定义分区数
    conf.set("spark.default.parallelism" , "5")
    val sc = new SparkContext(conf)

    //RDD的并行度 & 分区
    //makeRDD可以传入第二个参数，表示多少个分区
    //第二个参数可以 不传递，那么makeRDD方法会使用默认值
    //spark在默认情况下，从配置对象中获取配置参数
    //如果获取不到，那么使用totalCores属性，这个属性是当前环境的最大可用核数
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    //关闭连接
    sc.stop()


  }

}
