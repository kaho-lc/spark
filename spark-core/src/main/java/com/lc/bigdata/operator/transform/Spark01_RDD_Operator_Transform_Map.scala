package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**map转换算子
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark01_RDD_Operator_Transform_Map {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //转换函数
    def mapFunction(num: Int) = {
      num * 2
    }

    //这几种方法都可以，体现了scala的函数至简原则
    //    val value: RDD[Int] = rdd.map(mapFunction)
    //    val value: RDD[Int] = rdd.map((num :Int)=>num * 2)
    //    val value: RDD[Int] = rdd.map(_*2)
    val value: RDD[Int] = rdd.map(x => x * 2)

    value.collect().foreach(println)

    sc.stop()

  }

}
