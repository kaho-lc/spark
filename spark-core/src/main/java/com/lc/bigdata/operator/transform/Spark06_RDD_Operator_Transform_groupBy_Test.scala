package com.lc.bigdata.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** groupBy转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark06_RDD_Operator_Transform_groupBy_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-groupBy
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    //    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
    //      line => {
    //        val datas: Array[String] = line.split(" ")
    //        val time: String = datas(3)
    //        //        time.substring(0 , )
    //        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    //        val date: Date = sdf.parse(time)
    //        val sdf1 = new SimpleDateFormat("HH")
    //        val hour: String = sdf1.format(date)
    //        (hour, 1)
    //      }
    //    ).groupBy(_._1)

    val dataRDD: RDD[String] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(3)
      }
    )

    val value: RDD[(String, Int)] = dataRDD.map(
      line => {
        val hours: Array[String] = line.split(":")
        (hours(1), 1)
      }
    )
    val hoursRDD: RDD[(String, Iterable[(String, Int)])] = value.groupBy(x => x._1)

    hoursRDD.map {
      case (hour, iter) => (hour, iter.size)
    }.collect().foreach(println)

    //    timeRDD.map{
    //      case (hour , iter) => (hour , iter.size)
    //    }.collect().foreach(println)


    sc.stop()
  }
}
