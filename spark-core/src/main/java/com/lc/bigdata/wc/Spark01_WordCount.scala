package com.lc.bigdata.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-20
 * @time 20:17
 */
//noinspection ScalaDocParserErrorInspection
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    //todo  建立和spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //todo  执行业务操作

    //1.读取文件,获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")

    //2.将一行的数据进行拆分，形成一个一个的单词(分词操作)
    //扁平化：将整体拆分成个体的操作
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4.对分组后的数据进行转换
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //todo  关闭连接
    sc.stop()

  }
}
