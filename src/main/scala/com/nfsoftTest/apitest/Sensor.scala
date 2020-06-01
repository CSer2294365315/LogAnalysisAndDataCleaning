package com.nfsoftTest.apitest

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.client.{Request, Requests}


/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/3 16:19
  */

// 定义一个数据样例类，传感器id，采集时间戳，传感器温度
case class SensorReading( id: String, timestamp: Long, temperature: Double )

class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    // 定义保存到redis时的命令, hset sensor_temp sensor_id temperature
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temp" )
  }

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}

object Sensor {
  def main(args: Array[String]): Unit = {
    // 1. 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(5000)

    // 2. Source
    // Source1: 从集合读取
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    // Source2: 从文件读取
    val stream2 = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // Source3: kafka作为数据源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    // 3. Transformation
    val dataStream = stream3.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      // 升序数据分配时间戳
      //      .assignAscendingTimestamps(_.timestamp * 1000)
      // 乱序数据分配时间戳和水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(element: SensorReading): Long = {
        element.timestamp * 1000
      }
    })
//      .keyBy("id")
//      .reduce( (x, y) => SensorReading( x.id, x.timestamp.min(y.timestamp) + 5, x.temperature + y.temperature ) )

    // 根据温度高低拆分流
    val splitStream = dataStream.split( data => {
      if( data.temperature > 30 ) Seq("high") else Seq("low")
    } )

    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    val warning = high.map( data => ( data.id, data.temperature ) )
    val connected = warning.connect(low)

    val coMapStream = connected.map(
      warningData => ( warningData._1, warningData._2, "warning" ),
      lowData => ( lowData.id, "healthy" )
    )

    val union = high.union(low, all, high).map(data=>data.toString)

    // 统计每个传感器每3个数据里的最小温度
    val minTempPerWindow = dataStream.map( data=>(data.id, data.temperature) )
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
//      .countWindow(3, 1)
      .reduce( (data1, data2) => ( data1._1, data1._2.min(data2._2) ) )

    minTempPerWindow.print().setParallelism(1)

    // 4. Sink
//    high.print("high").setParallelism(1)
//    low.print("low").setParallelism(1)
//    all.print("all").setParallelism(1)
//    coMapStream.print("coMap").setParallelism(1)

    // kafka sink
//    union.print("union").setParallelism(1)
//    union.addSink( new FlinkKafkaProducer011[String]( "localhost:9092", "sinkTest", new SimpleStringSchema() ) )

//     redis sink
    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()
    dataStream.addSink( new RedisSink[SensorReading]( conf, new MyRedisMapper ) )

    // es sink
//    val httpHosts = new util.ArrayList[HttpHost]()
//    httpHosts.add( new HttpHost("localhost", 9200) )
//    // 创建esSink的Builder
//    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading]( httpHosts, new ElasticsearchSinkFunction[SensorReading] {
//      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//        println("saving data: " + t)
//        val json = new util.HashMap[String, String]()
//        json.put("sensor_id", t.id)
//        json.put("timestamp", t.timestamp.toString)
//        json.put("temperature", t.temperature.toString)
//
//        // 创建index request
//        val indexReq = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
//        requestIndexer.add(indexReq)
//        println("save successfully")
//      }
//    } )

//    dataStream.addSink( esSinkBuilder.build() )


    env.execute("API test")
  }
}
