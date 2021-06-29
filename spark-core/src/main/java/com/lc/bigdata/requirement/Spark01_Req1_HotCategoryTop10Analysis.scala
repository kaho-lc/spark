package com.lc.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 21:20
 */
object Spark01_Req1_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    //todo top10热门品类

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)


    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //2.统计品类的点击数量:(品类ID，点击数量)
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //3.统计品类的下单数量:(品类ID，下单数量)
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }

    ).reduceByKey(_ + _)

    //4.统计品类的支付数量:(品类ID，支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }

    ).reduceByKey(_ + _)

    //5.将品类进行排序，并且取出前10名
    //点击数量的排序，下单数量的排序，支付数量的排序
    //元组的排序:先比较第一个，如果第一个相同，再比较第二个，如果第二个相同，再比较第三个
    //(品类ID，(点击数量,下单数量,支付数量))
    //可以使用的算子cogroup = connect + group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val analisisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {

        var clickCnt: Int = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        var orderCnt: Int = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }

        var payCnt: Int = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD: Array[(String, (Int, Int, Int))] = analisisRDD.sortBy(_._2, false).take(10)

    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()


  }
}
