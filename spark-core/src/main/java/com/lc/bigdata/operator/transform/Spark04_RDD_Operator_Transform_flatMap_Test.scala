package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** flatMap转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark04_RDD_Operator_Transform_flatMap_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-flatMap
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case int: Int => List(int)
        }
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
