package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** repartition转换算子
 * 扩大分区
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark011_RDD_Operator_Transform_repartition {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-repartition
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)


    //coalesce算子可以扩大分区，但是不进行shuffle操作，是没有任何意义的，不起作用的
    //如果想要实现扩大分区的效果，需要使用shuffle操作
    //spark提供了简化的操作
    //缩减分区：coalesce，如果想要数据均衡可以采用shuffle
    //扩大分区：repartition，底层就是使用的coalesce方法
    //    val newRDD: RDD[Int] = rdd.coalesce(3 , true)
    val newRDD: RDD[Int] = rdd.repartition(3)

    newRDD.saveAsTextFile("output")


    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
