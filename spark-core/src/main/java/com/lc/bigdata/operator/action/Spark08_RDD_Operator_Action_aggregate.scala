package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark08_RDD_Operator_Action_aggregate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4) , 2)

    //aggregateByKey:初始值只会参与分区内计算
    //aggregate:初始值不仅仅参与分区内计算，还会参与分区间计算

    val result: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result)
    sc.stop()
  }
}
