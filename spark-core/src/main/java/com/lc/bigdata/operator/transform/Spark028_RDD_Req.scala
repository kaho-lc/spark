package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark028_RDD_Req {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 案例实操

    //1.获取原始数据
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")

    //2.将原始数据进行结构的转换，方便统计
    //时间戳，省份，城市，用户，广告 =>((省份，广告)，1)
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    //3.将转换结构后的数据进行分组聚合
    //((省份，广告)，1)=>((省份，广告)，sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    //4.将聚合的结果进行结构的转换
    //((省份，广告)，sum)=>(省份，(广告,sum))
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((privince, ad), sum) => (privince, (ad, sum))
    }

    //5.将转换结构后的数据根据省份进行分组
    //(省份，[(广告A,sumA),(广告B,sumB)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    //6.将分组后的数据组内排序
    val takeRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        //可迭代的集合不可以排序，可以将其转变成集合，之后再进行排序
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    //7.采集数据，打印到控制台
    takeRDD.collect().foreach(println)

    sc.stop()
  }
}
