package com.fanli.bigdata.test

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by laichao.wang on 2018/11/20.
  */
object Demo3 {

  def insertIntoDb(buffer: ArrayBuffer[ConsumerRecord[String, String]]) = {
    println("=======begin 2 db =========")
    for (record <- buffer){
      println(record.value())
    }
    println("=======end 2 db =========")
  }

  def main(args: Array[String]) {
    val  props = new Properties()
    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092")
    props.put("group.id", "test1")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    val topic = "apache_topic"
    consumer.subscribe( util.Arrays.asList("apache_topic"))

    val minBatchSize = 5
    val buffer = ArrayBuffer[ConsumerRecord[String, String]]()
    while (true) {
      val  records:ConsumerRecords[String, String] = consumer.poll(100)
      for (record <- records) {
        buffer.add(record);
      }
      if (buffer.size >= minBatchSize) {
        insertIntoDb(buffer)
        consumer.commitSync()
        buffer.clear()
      }
    }

  }
}
