package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** map转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark01_RDD_Operator_Transform_Map_Par {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    //1.RDD的计算一个分区内的数据是一个一个执行逻辑
    //  只有前面的一个数据全部的逻辑执行完毕后，才可以执行下一个数据
    //  分区内数据的执行是有序的(串行)

    //2.不同分区数据计算是无序的
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        println(">>>>>" + num)
        num
      }
    )
    val mapRDD1: RDD[Int] = mapRDD.map(
      num => {
        println("*****" + num)
        num
      }
    )
    mapRDD1.collect()

    sc.stop()

  }

}
