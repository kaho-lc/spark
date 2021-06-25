package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** mapPartitions转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark02_RDD_Operator_Transform_MapPartitions {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //类似于批处理，对每一个分区内的数据进行操作
    //mapPartitions可以以分区为单位进行数据转换操作，但是会将整个分区的数据加载到内存中进行引用
    //处理完的数据是不会被释放掉的，存在对象的引用。
    //在内存较小，数据量较大的场合下，容易出现内存的溢出
    //整个数据处理完之后才会释放

    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>")
        iter.map(_ * 2)
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()

  }

}
