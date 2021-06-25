package com.lc.bigdata.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author kaho-lc
 * @email lc1536328699@163.com
 * @date 2021-06-23
 * @time 14:16
 */
object Executor {
  def main(args: Array[String]): Unit = {

    //启动服务器来接收数据
    val server = new ServerSocket(9999)

    println("服务器启动，等待接收数据")

    //等待客户端的连接
    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task: Task = objIn.readObject().asInstanceOf[Task]

    val ints: List[Int] = task.conpute()
    println("计算节点计算的结果为: " + ints)

    objIn.close()
    client.close()
    server.close()


  }

}
