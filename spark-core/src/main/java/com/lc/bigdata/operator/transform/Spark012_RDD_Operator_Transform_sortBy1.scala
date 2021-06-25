package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** sortBy转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark012_RDD_Operator_Transform_sortBy1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //todo 算子-sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序排序，第二个参数可以改变排序的方式(降序)
    //sortBy默认情况是不会改变分区，但是中间存在shuffle操作
    //    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1)
    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt)
    //    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt, false)

    newRDD.collect().foreach(println)
    sc.stop()
  }
}
