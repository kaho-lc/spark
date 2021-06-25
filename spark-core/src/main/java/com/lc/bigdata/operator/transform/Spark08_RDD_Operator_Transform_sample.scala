package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** sample转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark08_RDD_Operator_Transform_sample {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1)

    //sample算子需要传递三个参数
    //1.第一个参数表示，抽取数据后是否将数据放回 ， true(放回) ， false(丢弃)
    //2.第二个参数
    //    如果是抽取不放回的场合：数据源中的每一条数据被抽取的概率，基准值的概念
    //    如果是抽取放回的场合：数据源中的每一条数据被抽取的可能次数。
    //3.表示抽取数据时，随机算法的种子,当种子确定后，随机数就确定了
    //如果不传递第三个参数，那么使用的是当前的系统时间，这样的话，每次抽取的随机数就会改变
//    println(rdd.sample(false, 0.4).collect().mkString(","))

    //在产生数据倾斜的时候可以使用sample

    println(rdd.sample(true, 2).collect().mkString(","))

    sc.stop()
  }
}
