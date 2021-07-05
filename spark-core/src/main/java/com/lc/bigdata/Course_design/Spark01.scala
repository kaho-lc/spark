package com.lc.bigdata.Course_design

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 不同年份召回轮胎的数量
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 18:42
 */
object Spark01 {
  def main(args: Array[String]): Unit = {

    //todo 获取spark连接
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Course design")
    val sc = new SparkContext(conf)

    //读取清洗后的数据(保存在datas/clean_datas.txt)
    val rdd: RDD[String] = sc.textFile("datas/clean_datas.txt")

    //使用map对rdd的结构进行转换
    val mapRDD: RDD[(String, Int)] = rdd.map(
      line => {
        //按照制表符切割数据得到数组
        val words: Array[String] = line.split("\t")
        (words(5), 1)
      }
    )

    //对转换后的rdd进行分组聚合
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    //使用map获得想要的结果类型--元组
    val mapRDD1: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => (word, iter.size)
    }

    //采集，并且打印输出到控制台
    mapRDD1.collect().foreach(println)
    //     mapRDD1.saveAsTextFile("E:\\data\\result1")


    //todo 关闭连接
    sc.stop()


  }


}
