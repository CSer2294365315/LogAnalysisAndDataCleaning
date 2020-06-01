package com.nfsoftTest.testCleanFromKafka

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class OrigJSON(
                     logType: String,
                     source: String, //logType+source = 日志拆分标识
                     line: String, //日志内容
                     sendTime: String //可以作为EventTime，添加到日志中
                   )

case class AppEmbeddedWebPage(
                               //Hera少一个ID字段，id=gzg_shouye.fulladpop.417.show
                               var user_id: Int = -1,
                               var guid: String = "",
                               var application: String = "",
                               var version: String = "",
                               var platform: String = "",
                               var page_id: String = "",
                               var objects: String = "",
                               var createtime: String = "",
                               var opentime: String = "",
                               var action_type: Int = -1,
                               var is_fanhui: Int = -1,
                               var scode_id: String = "",
                               var market_id: String = "",
                               var screen_direction: String = "",
                               var color: String = "",
                               var frameid: String = "",
                               var task_id: Int = -1,
                               var from_frameid: String = "",
                               var from_object: String = "",
                               var from_resourceid: String = "",
                               var to_frameid: String = "",
                               var to_resourceid: String = "",
                               var to_scode: String = "",
                               var order_id: String = "",
                               var activity_id: String = "",
                               //此处省略一个Map
                               //可作为EventTime
                               var logSentTime: String = ""
                             )


case class AppHeartBeatLog(
                            var user_id: Int = -1,
                            var guid: String = "",
                            var access_time: String = "",
                            var offline_time: String = "",
                            var download_channel: String = "",
                            var client_version: String = "",
                            var phone_model: String = "",
                            var phone_system: String = "",
                            var system_version: String = "",
                            var operator: String = "",
                            var network: String = "",
                            var resolution: String = "",
                            var screen_height: String = "",
                            var screen_width: String = "",
                            var mac: String = "",
                            //                            var sip: String = "",
                            //                            var province_value: String = "",
                            //                            var city_value: String = "",
                            var imei: String = "",
                            var iccid: String = "",
                            var meid: String = "",
                            var uuid: String = "",
                            var application: String = "",
                            var deveice_token: String = "",
                            var is_reload: Int = -1,
                            var android_id: String = "",
                            var idfa: String = "",
                            var oaid: String = "",
                            //可作为EventTime
                            var logSentTime: String = ""
                          )

case class ClientLog(
                      var user_id:Int = -1,
                      var guid:String = "",
                      var application:String = "",
                      var version:String = "",
                      var platform:String = "",
                      var start_object:String = "",
                      var objects:String = "",
                      var createtime:String = "",
                      var action_type:Int = -1,
                      var is_fanhui:Int = -1,
                      var scode_id:String = "",
                      var market_id:String = "",
                      var screen_direction:String = "",
                      var color:String = "",
                      var frameid:String = "",
                      var Type:Int = -1,
                      var qs_id:String = "",
                      var from_frameid:String = "",
                      var from_object:String = "",
                      var from_resourceid:String = "",
                      var to_frameid:String = "",
                      var to_resourceid:String = "",
                      //随机减去两个字段，测试数据比实际数据少两个字段
                      //var to_scode:String = "",
                      //var target_id:String = "",
                      //可作为EventTime
                      var logSentTime: String = ""
                    )

object testFlinkFromKafka {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "dsjtest01:9092")
    properties.setProperty("group.id", "consumerasdfsads023-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")


    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    var total: Long = 0L
    var ipOnlyFieldNum = 0L
    val stream = env.addSource(new FlinkKafkaConsumer011[String]("test0512", new SimpleStringSchema(), properties))
    stream.map(data => {
      val strAfterRep = data.replaceAll("\\\\", "")
      //print(strAfterRep + "\n\n\n\n\n")
      var arrAfterSplit: Array[String] = strAfterRep.split("&")
      var logJSON: String = arrAfterSplit(0)

      if (!logJSON.startsWith("[") && !logJSON.isEmpty) {
        try {
          var singleJSONObj: OrigJSON = JSON.parseObject(logJSON, classOf[OrigJSON])
          //提取出日志发送时间，加入到日志对象中
          var logSentTime: String = singleJSONObj.sendTime

          //TODO 按照日志的种类进行分流，不同日志采用不同的操作
          var logTypeVal = singleJSONObj.logType.toInt
          var sourceVal = singleJSONObj.source.toInt
          (logTypeVal, sourceVal) match {

            case (1, 1) | (1, 2) => {

              //TODO 股掌柜客户端心跳日志解析
              //通过测试，没有解释错误的问题
              var appHeartBeatLogStr: String = singleJSONObj.line
              var logSegArr: Array[String] = appHeartBeatLogStr.split("\\|", -1)
              var appHeartBeatLog: AppHeartBeatLog = new AppHeartBeatLog(
                logSegArr(0) match {
                  //userId
                  case "" => -1
                  case _ => logSegArr(0).toInt
                },
                logSegArr(1),
                logSegArr(2),
                logSegArr(3),
                logSegArr(4),
                logSegArr(5),
                logSegArr(6),
                logSegArr(7),
                logSegArr(8),
                logSegArr(9),
                logSegArr(10),
                logSegArr(11),
                logSegArr(12),
                logSegArr(13),
                logSegArr(14),
                logSegArr(15),
                logSegArr(16),
                logSegArr(17),
                logSegArr(18),
                logSegArr(19),
                logSegArr(20),
                logSegArr(21) match {
                  //is_reloaded
                  case "" => -1
                  case _ => logSegArr(21).toInt
                },
                logSegArr(22),
                logSegArr(23),
                logSegArr(24),
                logSentTime
              )
//              print(appHeartBeatLog + "\n\n\n")
              //测试通过
            }
            case (2, 1) => {
              //ios（股掌柜）
              //股掌柜客户端日志，暂定为（2,1），可能同时包括安卓和iso
              var appClientLogStr: String = singleJSONObj.line
              var logSegArr: Array[String] = appClientLogStr.split("\\|", -1)
              var clientLog: ClientLog = new ClientLog(
                logSegArr(0) match {
                  case "" => -1
                  case _ => logSegArr(0).toInt
                },
                logSegArr(1),
                logSegArr(2),
                logSegArr(3),
                logSegArr(4),
                logSegArr(5),
                logSegArr(6),
                logSegArr(7),
                logSegArr(8) match {
                  case "" => -1
                  case _ => logSegArr(8).toInt
                },
                logSegArr(9) match {
                  case "" => -1
                  case _ => logSegArr(9).toInt
                },
                logSegArr(10),
                logSegArr(11),
                logSegArr(12),
                logSegArr(13),
                logSegArr(14),
                logSegArr(15) match {
                  case "" => -1
                  case _ => logSegArr(15).toInt
                },
                logSegArr(16),
                logSegArr(17),
                logSegArr(18),
                logSegArr(19),
                logSegArr(20),
                logSegArr(21),
                //logSegArr(22),
                //logSegArr(23),
                logSentTime
              )
             //测试通过，没有脏数据
//               print(clientLog+ "\n\n\n")

            }
            case (2, 2) => {
              //安卓（股掌柜）
            }
            case (2, 3) => {
              //app网页（股掌柜app内嵌网页
              //网页端用户行为业务宽表
              //TODO 测试通过，没有解析错误，或者报错的问题，要加上try-catch
              var appEmbedWebLogStr: String = singleJSONObj.line
              var logSegArr: Array[String] = appEmbedWebLogStr.split("\\|", -1)
              var kvMap: util.HashMap[String, String] = new util.HashMap[String, String]()
              for (kv <- logSegArr) {
                var kvArr: Array[String] = kv.split("=")
                kvMap.put(kvArr(0), kvArr(1))
              }
              var webPageObj: AppEmbeddedWebPage = new AppEmbeddedWebPage(
                kvMap.getOrDefault("user_id", "-1").toInt,
                kvMap.getOrDefault("guid", ""),
                kvMap.getOrDefault("application", ""),
                kvMap.getOrDefault("version", ""),
                kvMap.getOrDefault("platform", ""),
                kvMap.getOrDefault("page_id", ""),
                kvMap.getOrDefault("objects", ""),
                kvMap.getOrDefault("createtime", ""),
                kvMap.getOrDefault("opentime", ""),
                kvMap.getOrDefault("action_type", "-1").toInt,
                kvMap.getOrDefault("is_fanhui", "-1").toInt,
                kvMap.getOrDefault("scode_id", ""),
                kvMap.getOrDefault("market_id", ""),
                kvMap.getOrDefault("screen_direction", ""),
                kvMap.getOrDefault("color", ""),
                kvMap.getOrDefault("frameid", ""),
                kvMap.getOrDefault("task_id", "-1").toInt,
                kvMap.getOrDefault("from_frameid", ""),
                kvMap.getOrDefault("from_object", ""),
                kvMap.getOrDefault("from_resourceid", ""),
                kvMap.getOrDefault("to_frameid", ""),
                kvMap.getOrDefault("to_resourceid", ""),
                kvMap.getOrDefault("to_scode", ""),
                kvMap.getOrDefault("order_id", ""),
                kvMap.getOrDefault("activity_id", ""),
                //添加上本条日志的send时间，可以作为EventTime
                logSentTime
              )
              //测试通过
              print(webPageObj+ "\n\n\n")
            }
            case (2, 4) => {
              //pc客户端（股掌柜）
            }
            case (2, 5) => {
              //手机浏览器 + PC端浏览器
            }
            case (2, 6) => {
              //微信小程序行为日志
            }
            case (4, 6) => {
              //微信小程序心跳日志
            }
            case (5, 4) => {
              //pc客户端（股掌柜）心跳日志
            }
            case _ => {
              //其他情况
            }
          }

        } catch {
          case es: Exception => {
            //TODO 此处是JSON不完整的数据，直接丢掉
          }
        }
      }
    })

    env.execute()
  }
}
