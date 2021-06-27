package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** foldByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark022_RDD_Operator_Transform_foldByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-foldByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("a", 4) , ("b" , 1) , ("b" , 2)
    ), 2)

    //如果聚合计算时分区内和分区间计算规则相同
    //spark提供了简化额方法foldByKey
    val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)

    foldRDD.collect().foreach(println)

    sc.stop()
  }
}
