package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark07_RDD_Operator_Action_takeOrdered {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(4, 3, 2, 1))

    //takeOrdered：数据排序后再取n个数据
    val ints: Array[Int] = rdd.takeOrdered(3)
    println(ints.mkString(","))

    sc.stop()
  }
}
