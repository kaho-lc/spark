package com.lc.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 21:20
 */
object Spark02_Req1_HotCategoryTop10Analysis1 {

  def main(args: Array[String]): Unit = {
    //todo top10热门品类

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    /**
     * 出现的问题：
     * actionRDD使用的过多：可以将其放在缓存中 cache
     * cogroup有可能存在shuffle，性能存在问题
     *
     * 数据源：
     * (品类ID，点击数量)
     * (品类ID，下单数量)
     * (品类ID，支付数量)
     * (品类ID，(点击数量,下单数量,支付数量))
     *
     * 可以转换成：
     * (品类ID，(点击数量 , 0 , 0))
     * (品类ID，(0 , 下单数量 , 0))
     * (品类ID，(0 , 0 , 支付数量))
     * (品类ID，(点击数量,下单数量,支付数量))
     *
     */


    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRDD.cache()


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
    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    //将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

    val analisisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[(String, (Int, Int, Int))] = analisisRDD.sortBy(_._2, false).take(10)

    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()


  }
}
