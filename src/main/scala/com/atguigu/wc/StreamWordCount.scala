package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/3 11:46
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2. source，动态读取数据，用socket发送
    val socketStream = env.socketTextStream(host, port).setParallelism(2)

    // 3. transformatiuon，转换操作
    // 进行word count处理，先分词，做flatmap，转换成(word, 1)二元组，最后做聚合统计
    val dataStream = socketStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    // 4. sink，输出
    dataStream.print().setParallelism(1)

    // 5. 启动executor
    env.execute()
  }
}
