package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** groupBy转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark06_RDD_Operator_Transform_groupBy {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-groupBy

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4) , 2)

    //groupBy将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //相同的key值的数据会放置在一个组中

    def groupFun(num :Int): Int ={
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFun)

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
