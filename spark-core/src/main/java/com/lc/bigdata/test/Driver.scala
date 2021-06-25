package com.lc.bigdata.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 14:16
 */
object Driver {
  def main(args: Array[String]): Unit = {

    //连接服务器
    val client = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val task = new Task()
    objOut.writeObject( task)
    objOut.flush()
    objOut.close()
    client.close()

    println("客户端数据发送完毕！")
  }
}
