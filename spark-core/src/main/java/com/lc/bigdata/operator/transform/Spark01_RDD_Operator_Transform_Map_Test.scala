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
object Spark01_RDD_Operator_Transform_Map_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    //拿到了apache.log的每一行的数据

    //
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )

    mapRDD.collect().foreach(println)

    //形成文件保存结果
//    mapRDD.saveAsTextFile("opuput/logs")

    sc.stop()

  }

}
