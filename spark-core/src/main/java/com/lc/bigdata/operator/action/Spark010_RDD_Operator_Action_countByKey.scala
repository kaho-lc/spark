package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark010_RDD_Operator_Action_countByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))
    //    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //
    //    println(intToLong)

    //countByKey：统计每种key出现的次数
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()

    println(stringToLong)

    sc.stop()
  }
}
