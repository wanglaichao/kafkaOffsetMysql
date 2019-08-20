package com.fanli.bigdata.multiple

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._

/**
  * Created by laichao.wang on 2018/11/26.
  */
object Demo6MutlThread {

  def main(args: Array[String]) {
    val  props = new Properties()
    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092")
    props.put("group.id", "test1")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val topic = "apache_topic"
    for( i <- 1 to 2 ){
      val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
      val listener: KafkaConsumerRebalanceListener = new KafkaConsumerRebalanceListener(consumer,topic)
      consumer.subscribe(util.Arrays.asList(topic),listener)
      while (true){
        val records: ConsumerRecords[String, String] = consumer.poll(1000)
        if( ! records.isEmpty)
        records.foreach(record=>{
          val partition: Int = record.partition()
          val topic: String = record.topic()
          printf("topic=%s,partition=%d,offset = %d, key = %s, value = %s%n",topic,partition, record.offset, record.key, record.value)
        })
      }
    }


  }

}
