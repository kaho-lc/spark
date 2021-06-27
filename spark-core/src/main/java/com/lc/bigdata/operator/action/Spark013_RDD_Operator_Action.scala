package com.lc.bigdata.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-26
 * @time 15:22
 */
object Spark013_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operetor")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List())

    val user = new User()

    //SparkException: Task not serializable 报错

    //RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    //闭包检测功能
    rdd.foreach(
      num => {
        println("age=" + (user.age + num))
      }
    )
    sc.stop()
  }
}

class User extends Serializable {
  //样例类在编译时会自动混入序列化特质(实现可序列化接口)
  //case class User() {
  //class User(){
  var age: Int = 30
}