package com.fanli.bigdata.test

import java.lang.Long
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
/**
  * Created by laichao.wang on 2018/11/21.
  * 可以自定义offset
  */
object Demo5 {


  def main(args: Array[String]) {
    val  props = new Properties()
    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092")
    props.put("group.id", "test1")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    val topic = "apache_topic"

    consumer.subscribe(Collections.singletonList( topic),new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partionts: util.Collection[TopicPartition]): Unit = {
        partionts.foreach(p=>{
          val partition0: TopicPartition = new TopicPartition(topic,p.partition())
          val offsets  = consumer.beginningOffsets(Collections.singletonList(partition0)).get(partition0)
          val endOffset: Long = consumer.endOffsets(Collections.singletonList(partition0)).get(partition0)
          val preSetOffset = 1l
          if ( offsets  > preSetOffset){
             printf("要设置的offset{%d}低于了offset{%d}最小值将从头记入,最大offset值是{%d}。\n",preSetOffset,offsets,endOffset)
            consumer.seek(partition0,offsets)
          }else{
            consumer.seek(partition0,preSetOffset)
          }
          //          val parts = util.Arrays.asList(partition0)
         })
      }

      override def onPartitionsRevoked(partionts: util.Collection[TopicPartition]): Unit = {
        partionts.foreach(p=>{
          val partitionNum  = p.partition()
          println("onPartitionsRevoked:partiontNum=>"+partitionNum)
        })
      }
    })



    while (true){
      val consumerRecords: ConsumerRecords[String, String] = consumer.poll(1000)
      if ( ! consumerRecords.isEmpty){
           consumerRecords.foreach(record=>{
             val partition: Int = record.partition()
             val topic: String = record.topic()
             printf("topic=%s,partition=%d,offset = %d, key = %s, value = %s%n",topic,partition, record.offset, record.key, record.value)
           })
      }
      consumer.commitSync()
    }
  }
}
