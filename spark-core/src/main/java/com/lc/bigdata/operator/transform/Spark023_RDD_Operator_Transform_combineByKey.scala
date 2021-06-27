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
object Spark023_RDD_Operator_Transform_combineByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-combineByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("a", 4), ("b", 1), ("b", 2)
    ), 2)

    //combineByKey：方法需要三个参数
    //第一个参数：将相同key的第一个数据进行结构的转换，实现操作
    //第二个参数：分区内的计算规则
    //第三个参数：分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
