package com.nfsoftTest.testCleanFromKafka

object testHeartBeatLog {
  def main(args: Array[String]): Unit = {
    val log : String = "37906|0127761205d675976ddba59f0b4550b3|gzg|2.8.27|ios|gzg_shouye|1590125659|0|0||||white|home_1.0.0|2||home_1.0.0|gzg_shouye.shouye|||||0"
    var strings: Array[String] = log.split("\\|", -1)
    print("length::"+strings.length+"\n")
    for (str <- strings){
      print(str+"\n")
    }
  }
}
