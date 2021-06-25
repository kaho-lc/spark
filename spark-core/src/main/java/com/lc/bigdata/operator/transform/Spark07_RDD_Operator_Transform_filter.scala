package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** filter转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark07_RDD_Operator_Transform_filter {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-filter

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.filter(_ % 2 == 0).collect().foreach(println)

    sc.stop()
  }
}
