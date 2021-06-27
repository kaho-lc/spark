package com.lc.bigdata.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-20
 * @time 20:17
 */
//noinspection ScalaDocParserErrorInspection
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    //todo  建立和spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //todo  执行业务操作
    wordCount1(sc)

    //todo  关闭连接
    sc.stop()

  }


  //groupBy
  def wordCount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val group: RDD[(String, Iterable[String])] = word.groupBy(word => word)

    val wordCount: RDD[(String, Int)] = group.mapValues(
      iter => iter.size
    )
  }

  //groupByKey
  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()

    val wordCount: RDD[(String, Int)] = group.mapValues(
      iter => iter.size
    )
  }

  //reduceByKey
  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
  }

  //aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  //foldByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }

  //combineByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val wordcount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
  }

  //countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordOne: RDD[(String, Int)] = word.map((_, 1))

    val wordCount: collection.Map[String, Long] = wordOne.countByKey()

  }

  //  countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val wordCount: collection.Map[String, Long] = word.countByValue()

  }


  def wordCount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val word: RDD[String] = rdd.flatMap(_.split(" "))

    val mapWord: RDD[mutable.Map[String, Long]] = word.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount: mutable.Map[String, Long] = mapWord.reduce(
      (map1, map2) => {
        map1
      }
    )

    println(wordCount)
  }


}
