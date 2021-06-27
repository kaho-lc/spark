package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** reduceByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark018_RDD_Operator_Transform_reduceByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-reduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作，都是两两聚合,spark基于scala开发的，所有spark的聚合也是两两聚合
    //reduceByKey中，如果key的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => x + y)

    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}
