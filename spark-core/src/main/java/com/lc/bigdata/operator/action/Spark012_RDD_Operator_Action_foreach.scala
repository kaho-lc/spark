package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark012_RDD_Operator_Action_foreach {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("*****")
    //foreach 其实是Executor端内存数据打印
    rdd.foreach(println)

    //算子：Operator（操作）
    //    RDD的方法和Scala集合对象的方法不一样
    //    集合对象的方法都是在同一个节点的内存中完成的
    //    RDD的方法可以将计算逻辑发送到Executor端(分布式节点)执行
    //    为了区分不同的处理效果，所以就RDD的方法称之为算子
    //    RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行的

    sc.stop()
  }
}
