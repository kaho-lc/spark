package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark03_RDD_Operator_Action_collect {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //collect会将不同分区的数据按照分区顺序采集到Driver端的内存中，形成数组
    val array: Array[Int] = rdd.collect()
    println(array.mkString(","))
    sc.stop()
  }
}
