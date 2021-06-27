package com.lc.bigdata.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** aggregateByKey转换算子
 *
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-24
 * @time 20:04
 */
object Spark021_RDD_Operator_Transform_aggregateByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    //    todo key-value类型
    //    todo 算子-aggregateByKey

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), ("a", 3), ("b", 1), ("b", 2)
    ), 2)

    //aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
    //    rdd.aggregateByKey("")(_+_ , _+_)

    //获取相同key的数据的平均值=>(a , 3),(b , 3)
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cut) => {
        num / cut
      }
    }

    resultRDD.collect().foreach(println)
    
    sc.stop()
  }
}
