package com.lc.bigdata.Course_design

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 不同厂商召回轮胎的数量
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 18:42
 */
object Spark02 {
  def main(args: Array[String]): Unit = {

    //todo 获取spark连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Course design")
    val sc = new SparkContext(conf)

    //读取清洗后的数据(保存在datas/clean_datas.txt)
    val rdd: RDD[String] = sc.textFile("datas/clean_datas.txt")

    //获取到每一行的公司名
    val mapRDD: RDD[Array[String]] = rdd.map(
      line => {
        //按照制表符切割数据得到数组
        val words: Array[String] = line.split("\t")
        words
      }
    )

    //对一些数据进行筛选操作，因为他的字段没有十五个
    val filterRDD: RDD[Array[String]] = mapRDD.filter(_.length == 15)

    //将所获取的数据以元组的形式保存
    val mapRDD1: RDD[(String, Int)] = filterRDD.map(
      arr => (arr(13), 1)
    )

    //对数据进行分组聚合,返回的是公司名和个数迭代器的元组
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    val mapRDD2: RDD[(String, Int)] = groupRDD.map {
      case (words, iter) => {
        (words, iter.size)
      }
    }


    mapRDD2.collect().foreach(println)
    //将结果采集，并且保存到文件
    //    mapRDD2.saveAsTextFile("E:\\data\\result2")

    //todo 关闭连接
    sc.stop()
  }

}
