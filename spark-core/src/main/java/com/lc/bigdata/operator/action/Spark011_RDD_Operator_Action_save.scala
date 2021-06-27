package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark011_RDD_Operator_Action_save {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ), 2)

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")

    //saveAsSequenceFile:要求数据的格式必须为key-value 类型
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }
}
