package com.lc.bigdata.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 16:12
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建rdd
    //从内存中创建RDD，将内存中集合的数据作为处理的数据源

    val seq = Seq[Int](1 , 2 , 3 , 4)
//    val rdd: RDD[Int] = sc.parallelize(seq)
    //makeRDD方法在底层实现时就是调用了rdd对象的parallelize方法
    val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)


    //关闭连接
    sc.stop()




  }

}
