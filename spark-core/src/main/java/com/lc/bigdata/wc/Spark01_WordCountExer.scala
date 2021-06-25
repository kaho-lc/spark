package com.lc.bigdata.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-22
 * @time 19:15
 */
object Spark01_WordCountExer {
  def main(args: Array[String]): Unit = {

    //建立与spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCountExer")
    val sc = new SparkContext(sparkConf)

    //读取文件:获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")

    //将每一行的数据按照空格拆分
    val words: RDD[String] = lines.flatMap(x => x.split(" "))
    //也可以写成下面这种形式
    //    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //对分组结果进行转换

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //将结果采集并且打印输出
    wordToCount.collect().foreach(println)

    sc.stop()
  }


}
