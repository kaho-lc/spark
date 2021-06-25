package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** coalesce转换算子
 * 缩减分区
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark010_RDD_Operator_Transform_coalesce {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce方法默认不会将分区的数据打乱组合
    //这种情况缩减分区可能导致数据不均衡，产生数据倾斜
    //如果想让数据均衡，可以进行shuffle处理
//    val newRDD: RDD[Int] = rdd.coalesce(2)

    //第二个参数，可以让数据变得均衡
    val newRDD: RDD[Int] = rdd.coalesce(2, true)

    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
