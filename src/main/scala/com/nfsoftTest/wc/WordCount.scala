package com.nfsoftTest.wc

import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2. source，从文件中读取数据
    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDS = env.readTextFile(inputPath)

    // 3. transformatiuon，转换操作
    // 进行word count处理，先分词，做flatmap，转换成(word, 1)二元组，最后做聚合统计
    val wordCountDS = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    // 4. sink，输出
    wordCountDS.print()
  }
}
