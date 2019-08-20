package com.fanli.bigdata.test

import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

//com.fanli.bigdata.test.ConsumerFixOffset  1 37245138419
object ConsumerFixOffset {

  def main(args: Array[String]): Unit = {
    var cycleTimes = 10
    if(args.length >=1){
      cycleTimes = args(0).toInt
    }
    var  beginOffset:Long = 1
    if(args.length >=2){
      beginOffset = args(1).toLong
    }

    val  props = new Properties()
//    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092")
    props.put("bootstrap.servers", "192.168.3.190:9092,192.168.3.191:9092")
    props.put("group.id", "apache_wash_debug_kafka2hdfs1")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val  consumer:KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    val topic = "apache_topic"

    val partition5: TopicPartition = new TopicPartition(topic,5)
    val parts = util.Arrays.asList(
      partition5)
    consumer.assign(parts)
    consumer.seek(partition5,beginOffset)


   for(i<- 1 to cycleTimes ){
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
