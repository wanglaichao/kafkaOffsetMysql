package com.fanli.bigdata.test

import java.lang.Long
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by laichao.wang on 2018/11/20.
  */
object Demo1 {


  def main(args: Array[String]) {
    val  props = new Properties()
    props.put("bootstrap.servers", "10.0.5.163:9092,10.0.4.166:9092")
    props.put("group.id", "debug_apache_topic")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)


    val topic = "apache_topic"
    consumer.subscribe( util.Arrays.asList("apache_topic"))
    val partition5: TopicPartition = new TopicPartition(topic,5)
    val metadata0: OffsetAndMetadata = new OffsetAndMetadata(0)

    /*val partition0: TopicPartition = new TopicPartition(topic,0)
    val partition1: TopicPartition = new TopicPartition(topic,1)
    val partition2: TopicPartition = new TopicPartition(topic,2)
    val partition3: TopicPartition = new TopicPartition(topic,3)
    val partition4: TopicPartition = new TopicPartition(topic,4)
    val partition6: TopicPartition = new TopicPartition(topic,6)
    val partition7: TopicPartition = new TopicPartition(topic,7)
    val partition8: TopicPartition = new TopicPartition(topic,8)
    val partition9: TopicPartition = new TopicPartition(topic,9)*/
    consumer.commitSync(mapAsJavaMap(Map(partition5->metadata0)))
    val beginningOffsets: util.Map[TopicPartition, Long] = consumer.beginningOffsets(util.Arrays.asList(partition5))
    while (true) {
      val  records:ConsumerRecords[String, String] = consumer.poll(1000)
      records.iterator().foreach(record=>{
        val partition: Int = record.partition()
        val topic: String = record.topic()

        printf("topic=%s,partition=%d,offset = %d, key = %s, value = %s%n",topic,partition, record.offset, record.key, record.value)
      })
      /*import scala.collection.JavaConversions._
      records.records().foreach(record=>{
        println("offset = %d, key = %s, value = %s%n", record.offset, record.key, record.value);
      })*/

    }


  }
}
