package com.fanli.bigdata.multiple

import java.util
import java.util._
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by laichao.wang on 2018/11/29.
  * @param args
  * @param blcokQueue  消息队列
  * @param dbproperty   数据库配置
  * @param isPrintBlockInfo 定时打印队列信息
  * @param isStartOffsetRecords 将offset快照定时刷新到数据库
  */
class KafkaConsumerBasic(args: Array[String]
                         , blcokQueue:BlockingQueue[java.util.List[ConsumerRecord[String, String]]]
                         ,dbproperty:String
                         ,isPrintBlockInfo:Boolean=true,
                         isStartOffsetRecords:Boolean=true)
    extends Thread
  {
  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  def statConsumerRecords() = {
      val kafkaProperties: Properties = KafkaPropertyBasic.getKafkaProperties(args)
      val kafkaConsumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](kafkaProperties)
      val topic = kafkaProperties.getProperty(EnumKafkaProperty.UnKafkaProperty.UNKAFKA_TOPIC.toString)
      val group: String = kafkaProperties.getProperty(EnumKafkaProperty.UnKafkaProperty.UNKAFKA_GROUP.toString)
      val mysql_set: String = kafkaProperties.getProperty(EnumKafkaProperty.UnKafkaProperty.UNKAFKA_MYSQL_OFFSET_RESET.toString)
      val kafkaConsumerRebalanceListener: KafkaConsumerRebalanceListener = KafkaConsumerRebalanceListener(kafkaConsumer,topic)
      val buffer: ArrayBuffer[String] = ArrayBuffer()
      topic.split(",").foreach(buffer.+=(_))
      kafkaConsumer.subscribe(buffer,kafkaConsumerRebalanceListener)
      while (KafkaConsumerBasic.consumerRunning.get()){
          val consumerRecords: ConsumerRecords[String, String] = kafkaConsumer.poll(kafkaProperties.getProperty("kafka.poll.time.out").toLong)
          if ( ! consumerRecords.isEmpty){
            consumerRecords.partitions().foreach(p =>{
              val records = consumerRecords.records(p)
              if(!records.isEmpty){
                blcokQueue.put(records)
                //records.foreach(blcokQueue.put(_))
                val endOffset:Long = kafkaConsumer.endOffsets(Collections.singletonList(p)).get(p).longValue()
                val offsetKey: OffsetKey = OffsetKey(topic,p.partition(),group)
                val offsetValue = MysqlKafkaOffset.kafkaOffsetRecord.get(offsetKey).getOrElse(null)
                val nowOffset = offsetValue.offset + records.size()
                val lag = endOffset - nowOffset
                MysqlKafkaOffset.kafkaOffsetRecord.put(offsetKey,OffsetValue(nowOffset,lag,endOffset))
              }
            })
            MysqlKafkaOffset.flush()
          }
      }

    logger.info("kafkaConsumer 消费结束")
    kafkaConsumer.close()

  }

  val timer: Timer = new Timer(true)
  def printBlockQueueInfo() = {
    //定时打印数据  设置成守护线程
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        logger.info("time=>"+new Date()+",block-Size=>"+blcokQueue.size()+",remainingCapacity=>"+blcokQueue.remainingCapacity())
      }
    },60*1000,60*1000)
  }


  def timerFlushPartitionsOffset2db() = {
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        if (KafkaConsumerRebalanceListener.hasFinishedPartitionsAssigned){
          MysqlKafkaOffset.flushPartitionsOffset2db()
        }else{
          logger.warn("PartitionsAssigned 还未结束！")
        }
      }

    },10*1000,60*1000*10)

  }

  override def run(): Unit = {

    //初始化 dbproperty
    MysqlKafkaOffset.dbPropryName=dbproperty
    if(isPrintBlockInfo){
      printBlockQueueInfo()
    }
    if(isStartOffsetRecords)
      timerFlushPartitionsOffset2db()

    this.statConsumerRecords()
  }
}
object KafkaConsumerBasic{
    val consumerRunning=new AtomicBoolean(true)

  /**
    * @param args
    * @param blcokQueue  消息队列
    * @param dbproperty   数据库配置
    * @param isPrintBlockInfo 定时打印队列信息
    * @param isStartOffsetRecords 将offset快照定时刷新到数据库
    * @param isDaemon 是否以伴生对象的形式存活
    */
    def startConumer(args: Array[String]
    , blcokQueue: BlockingQueue[util.List[ConsumerRecord[String, String]]]
    , dbproperty: String
    , isPrintBlockInfo:Boolean=true,isStartOffsetRecords:Boolean=true,isDaemon:Boolean=true)={
     val consumerBasic: KafkaConsumerBasic =
       new KafkaConsumerBasic(args, blcokQueue, dbproperty, isPrintBlockInfo, isStartOffsetRecords)
      consumerBasic.setDaemon(isDaemon)
      consumerBasic.start()
  }
}
