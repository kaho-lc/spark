package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** aggregateByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark020_RDD_Operator_Transform_aggregateByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-aggregateByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)

    //a[1 , 2] , a[3 , 4]
    //(a , 2) , (a , 4)

    //aggregateByKey存在函数的柯里化，有两个参数列表
    //第一个参数列表需要传递一个参数，表示为初始值
    //    主要用于当碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递两个参数
    //    第一个参数表示分区内计算规则
    //    第二个参数表示分区间计算规则
    rdd.aggregateByKey(0)(
      (x, y) => Math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)
    sc.stop()
  }
}
