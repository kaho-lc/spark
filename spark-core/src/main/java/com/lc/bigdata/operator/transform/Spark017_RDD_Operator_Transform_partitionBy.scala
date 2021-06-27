package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/** partitionBy转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark017_RDD_Operator_Transform_partitionBy {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-partitionBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(x => (x, 1))

    //RDD=>PairRDDFunctions
    //隐式转换(二次编译)

    //partitionBy根据指定的分区规则对数据进行重分区

    //如果重分区的分区器和当前的RDD的分区器一样的话什么都不做
    //
    val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))
    newRDD.partitionBy(new HashPartitioner(2))
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
