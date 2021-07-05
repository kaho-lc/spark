package com.lc.bigdata.Course_design

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 奥迪品牌的不同ModelYear召回的数量
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 18:42
 */
object Spark03 {
  def main(args: Array[String]): Unit = {

    //todo 获取spark连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Course design")
    val sc = new SparkContext(conf)

    //读取清洗后的数据(保存在datas/clean_datas.txt)
    val rdd: RDD[String] = sc.textFile("datas/clean_datas.txt")

    //筛选出奥迪公司
    val filterRDD: RDD[String] = rdd.filter(_.contains("AUDI"))


    val mapRDD: RDD[(String, Int)] = filterRDD.map(
      line => {
        //按照制表符切割数据得到数组
        val words: Array[String] = line.split("\t")
        (words(5), 1)
      }
    )

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    val mapRDD1: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => (word, iter.size)
    }

    mapRDD1.collect().foreach(println)
    //将结果采集并且保存到文件
    //    mapRDD1.saveAsTextFile("E:\\data\\result3")

    //todo 关闭连接
    sc.stop()


  }


}
