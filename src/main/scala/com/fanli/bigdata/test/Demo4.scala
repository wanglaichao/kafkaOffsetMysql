package com.fanli.bigdata.test

import java.util
import java.util.{Collections, Properties}

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

/**
  * Created by laichao.wang on 2018/11/20.
  */
object Demo4 {


  def main(args: Array[String]) {
    val  props = new Properties()
    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092")
    props.put("group.id", "test1")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    val topic = "apache_topic"
    val partition0: TopicPartition = new TopicPartition(topic,0)

    val partition1: TopicPartition = new TopicPartition(topic,1)
    val partition2: TopicPartition = new TopicPartition(topic,2)
    val partition3: TopicPartition = new TopicPartition(topic,3)
    val partition4: TopicPartition = new TopicPartition(topic,4)
    val partition5: TopicPartition = new TopicPartition(topic,5)
    val partition6: TopicPartition = new TopicPartition(topic,6)
    val partition7: TopicPartition = new TopicPartition(topic,7)
    val partition8: TopicPartition = new TopicPartition(topic,8)
    val partition9: TopicPartition = new TopicPartition(topic,9)

    val parts = util.Arrays.asList(partition0,
        partition1,
        partition3,
        partition4,
        partition5,
        partition6,
        partition7,
        partition8,
        partition9,
        partition2)

    consumer.assign(parts)
    consumer.seekToEnd(parts)

    while (true) {
      val  records:ConsumerRecords[String, String] = consumer.poll(1000)
      for (partition <-records.partitions()){
        val partitionRecords: util.List[ConsumerRecord[String, String]] = records.records(partition)
        for( record <- partitionRecords){
          println("partiiont="+record.partition() +"#offset="+record.offset() + "#value=" + record.value())
        }
        val offset: Long = partitionRecords.get(partitionRecords.size()-1).offset()
        consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(offset+1)))
      }
    }

  }
}
