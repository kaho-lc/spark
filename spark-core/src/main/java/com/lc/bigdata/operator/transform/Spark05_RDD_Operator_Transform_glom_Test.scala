package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** glom转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark05_RDD_Operator_Transform_glom_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //所有分区的最大值求和(6)
    //将rdd中的分区数据转换成数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    //将每个分区数组中的最大值取出
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )

    //计算分区数组最大值的和
    println(maxRDD.collect().sum)

    sc.stop()
  }
}
