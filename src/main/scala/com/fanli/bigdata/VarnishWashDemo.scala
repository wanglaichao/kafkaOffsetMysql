/*
package com.fanli.bigdata

import java.util.{Date, Locale, Properties}
import java.util.concurrent.{ExecutorService, Executors}

import com.fasterxml.jackson.core.JsonParseException
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by laichao.wang on 2018/11/11.
  * com.fanli.test.VarnishWashDemo
  */
object VarnishWashDemo {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("rebalance.max.retries", "10");
    props.put("rebalance.backoff.ms", "1200");
    props.put("fetch.message.max.bytes", "10485760")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //从头消费
    props.put("auto.offset.reset", "largest") //smallest [largest, smallest, none]中的一个，默认是"latest
    logger.warn("@props@" + props)
    val config = new ConsumerConfig(props)
    config
  }
  def main(args: Array[String]) {

    val config =
      createConsumerConfig("10.0.5.164:2181/kafka", "apache_topic_test")
    val consumer = Consumer.create(config)
    val executor: ExecutorService = Executors.newFixedThreadPool(1)

    def shutdown() = {
      if (consumer != null)
        consumer.shutdown()
      if (executor != null)
        executor.shutdown()
    }


  val topic = "apache_topic"
    val topicCountMap = Map(topic -> 1)
    logger.warn("@topicCountMap@" + topicCountMap)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get
    for (stream <- streams) {
      executor.execute(new TWashRun(stream, 1))
    }
  }
}

class TWashRun(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int) extends  Runnable {
  val logger:Logger = LoggerFactory.getLogger(this.getClass)
  def parseAppLog(msg: String,key: String) = {
    try {
      logger.info(key+"===>"+msg)
    }catch {
      case jp:JsonParseException =>
      case indexOut:StringIndexOutOfBoundsException=>
      case e:Exception=> {
        logger.error("解析失败",e)
      }
      case _:Throwable =>
    }
    null
  }

  def run {
    logger.warn("begin...")
    val it = stream.iterator()
    while (it.hasNext()) {
     val  next = it.next()
      logger.info("===>"+next)
      if(next.key() != null){
        val key = new String(next.key())
      }
//      logger.info(key)
      val msg = new String(next.message())
      logger.info("===>"+msg)
//      logger.info(msg)
      //      parseAppLog(msg,key)
    }
    logger.error("Shutting down Thread: " + threadNumber)
  }

}

*/
