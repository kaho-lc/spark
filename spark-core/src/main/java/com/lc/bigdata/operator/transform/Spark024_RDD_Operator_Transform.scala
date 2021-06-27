package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** combineByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark024_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-combineByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("a", 4), ("b", 1), ("b", 2)
    ), 2)

    rdd.reduceByKey(_ + _)//wordcount

    rdd.aggregateByKey(0)(_ + _, _ + _)//wordcount

    rdd.foldByKey(0)(_ + _)//wordcount

    rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)//wordcount

    sc.stop()
  }
}
