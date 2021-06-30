package com.lc.bigdata.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-28
 * @time 21:20
 */
object Spark04_Req1_HotCategoryTop10Analysis3 {

  def main(args: Array[String]): Unit = {
    //todo top10热门品类

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(conf)

    /**
     * 出现的问题：
     * 仍然存在shuffle操作:因为存在reduceByKey
     * 使用累加器来避免shuffle操作
     *
     */


    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val acc = new HotCategoryAccumulator

    sc.register(acc, "hotCategory")

    //2.将数据转换结构
    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击的场合
          acc.add(datas(6), "click")
        } else if (datas(8) != "null") {
          //下单的场合
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add(id, "order")
            }
          )
        } else if (datas(10) != "null") {
          //支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add(id, "pay")
            }
          )
        }
      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)


    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          }
          else {
            false
          }
        }
        else {
          false
        }
      }
    )


    //5.将结果采集到控制台打印出来
    sort.take(10).foreach(println)

    sc.stop()
  }

}

//自定义累加器
//1.继承AccumulatorV2，定义泛型
//    IN：(品类ID，行为类型)
//    OUT：mutable.Map[String ,HotCategory]
class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
  private val hcMap = mutable.Map[String, HotCategory]()

  //重写方法
  override def isZero: Boolean = {
    hcMap.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new HotCategoryAccumulator
  }

  override def reset(): Unit = {
    hcMap.clear()
  }

  override def add(v: (String, String)): Unit = {
    val cid: String = v._1
    val actionType: String = v._2
    val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
    if (actionType == "click") {
      category.clickCount += 1
    } else if (actionType == "order") {
      category.orderCount += 1
    } else if (actionType == "pay") {
      category.payCount += 1
    }

    hcMap.update(cid, category)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    val map1: mutable.Map[String, HotCategory] = this.hcMap
    val map2: mutable.Map[String, HotCategory] = other.value

    map2.foreach {
      case (cid, hc) => {
        val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
        category.clickCount += hc.clickCount
        category.orderCount += hc.orderCount
        category.payCount += hc.payCount
        map1.update(cid, category)
      }
    }
  }

  override def value: mutable.Map[String, HotCategory] = {
    hcMap
  }
}

case class HotCategory(cid: String, var clickCount: Int, var orderCount: Int, var payCount: Int)
