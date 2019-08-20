package com.fanli.bigdata.multiple

import java.util.{Date, Properties, Timer, TimerTask}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.fanli.bigdata.until.TimeUtil
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by laichao.wang on 2018/11/29.
  * com.fanli.bigdata.multiple.KafkaTest
  */
object KafkaTest {
  val logger:Logger = LoggerFactory.getLogger(this.getClass);
  // -topics apache_topic  -group
  //  -mysql_offset_reset  largest/earliest/absearliest(每次启动都将offset放置最前)/abslargest(每次启动都将offset放置最大)
  // -topic apache_topic  -group  a1  -mysql_offset_reset  lagest -bootstrap_servers  10.0.5.163:9092,10.0.5.164:9092
  def main(args: Array[String]) {
    this.getClass.getClassLoader.getResource("log4j.xml")
    //获取kafka的配置属性
//    val kafkaProperties: Properties = KafkaPropertyBasic.getKafkaProperties(args)
    val blockSize = 100
    val records: BlockingQueue[java.util.List[ConsumerRecord[String, String]]]
    = new LinkedBlockingQueue[java.util.List[ConsumerRecord[String, String]]](blockSize)
//    val dbProperty = "100_60.properties"
    val dbProperty = "test_162.properties"
    KafkaConsumerBasic.startConumer(args,records,dbProperty)
//    val kafkaConsumerDaemon = new KafkaConsumerBasic(,isStartOffsetRecords = true).start()
    var recordsSize = 0

//    printRs(recordsSize)
    val currentTimeMillis: Long = System.currentTimeMillis()
    while (KafkaConsumerBasic.consumerRunning.get()){
        val record:java.util.List[ConsumerRecord[String, String]] = records.poll(2,TimeUnit.SECONDS)
       if(record != null){
         recordsSize +=record.size()
         logger.info("partition=>"+record.get(0).partition()+",record size =>"+record.size())
       }
    }
    val endTime =  System.currentTimeMillis()
    logger.warn("ENDIng ... ")
  }

  def printRs(recordsSize: Int) = {
    val timer: Timer = new Timer(true)
    //定时打印数据  设置成守护线程
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        logger.info("消费记录数=>"+recordsSize)
      }
    },1*1000,1*1000)

  }

}
