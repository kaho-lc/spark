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
object Spark07_RDD_Operator_Transform_filter_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-filter

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val dataRDD: RDD[(String, String)] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(6), datas(3))
      }
    )

    val value: RDD[String] = dataRDD.filter(_._2.startsWith("17/05/2015")).map(_._1)
    value.collect().foreach(println)


    sc.stop()
  }
}
