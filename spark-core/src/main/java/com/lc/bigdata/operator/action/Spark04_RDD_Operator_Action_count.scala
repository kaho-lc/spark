package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark04_RDD_Operator_Action_count {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //count统计数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)
    sc.stop()
  }
}
