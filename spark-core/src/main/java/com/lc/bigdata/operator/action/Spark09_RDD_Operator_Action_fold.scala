package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark09_RDD_Operator_Action_fold {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //fold:当分区间和分区内的操作一样时，可以使用fold

    val result: Int = rdd.fold(10)(_ + _)
    println(result)
    sc.stop()
  }
}
