package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** groupByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark019_RDD_Operator_Transform_groupByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-groupByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    //groupByKey将数据源中的数据，相同的key的数据分在一个组中，形成一个对偶元组
    //    元组中的第一个元素就是key，第二个元素就是相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    groupRDD.collect().foreach(println)

    val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)


    sc.stop()
  }
}
