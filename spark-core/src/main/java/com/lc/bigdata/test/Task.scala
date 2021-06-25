package com.lc.bigdata.test

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 14:24
 */
class Task extends Serializable {

  val datas = List(1, 2, 3, 4)

  //  val logic = (num :Int)=> num * 2
  val logic: (Int) => Int = _ * 2

  def conpute() = {
    datas.map(logic)
  }

}
