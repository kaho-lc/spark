package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** zip转换算子
 * 拉链
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark016_RDD_Operator_Transform_zip1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo 算子-zip
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    //拉链
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    //Can't zip RDDs with unequal numbers of partitions: List(2, 4) 报错
    //两个数据源要求分区数量要保持一致

    //Can only zip RDDs with same number of elements in each partition
    //两个数据源要求分区中数据数量保持一致
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
