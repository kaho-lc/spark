package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** zip转换算子
 *  拉链
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark016_RDD_Operator_Transform_zip {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo 算子-zip
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    //拉链
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

    println(rdd3.collect().mkString(","))

    //如果两个RDD数据类型不一样怎么办？
    //交集，并集，差集要求两个数据源的数据类型保持一致
    //拉链操作两个数据源的类型可以不一致
    val rdd4: RDD[String] = sc.makeRDD(List("3", "4", "5", "6"))





    sc.stop()
  }
}
