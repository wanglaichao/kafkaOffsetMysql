package com.fanli.bigdata.multiple

import java.util
import java.util.Collections
import com.fanli.bigdata.exceptions.KafkaConsumerException
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
/**
  * Created by laichao.wang on 2018/11/26.
  */
class KafkaConsumerRebalanceListener(val consumer:KafkaConsumer[String, String],val topic:String) extends ConsumerRebalanceListener{
  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def onPartitionsAssigned(partionts: util.Collection[TopicPartition]): Unit = {

    val group: String = KafkaPropertyBasic.props.getProperty(EnumKafkaProperty.UnKafkaProperty.UNKAFKA_GROUP.toString)
    val mysql_set: String = KafkaPropertyBasic.props.getProperty(EnumKafkaProperty.UnKafkaProperty.UNKAFKA_MYSQL_OFFSET_RESET.toString)
    MysqlKafkaOffset.initOffset(topic,group)
    partionts.foreach(p=>{
      val partition0: TopicPartition = new TopicPartition(topic,p.partition())
      val beginOffset  = consumer.beginningOffsets(Collections.singletonList(partition0)).get(partition0)
      val endOffset: Long = consumer.endOffsets(Collections.singletonList(partition0)).get(partition0)
      var finalOffset = beginOffset
      val offsetValue: OffsetValue = MysqlKafkaOffset.kafkaOffsetRecord.get(OffsetKey(topic,p.partition(),group)).getOrElse(OffsetValue(0,0,0))
      //分钟级别匹配
      val minPattern = "last([\\d]+)m".r

      mysql_set match {
        //  earliest|lagest 按照现有的offset进行匹配
        case "earliest"=>{
            if(offsetValue.offset > beginOffset){
              finalOffset = offsetValue.offset
            }else{
              logger.warn(s"设置的offset{${offsetValue.offset}低于了offset{$beginOffset}最小值将从头记入,最大offset值是{$endOffset}。")
            }
        }
        case "largest"  =>{
          if(offsetValue.offset > beginOffset){
            finalOffset = offsetValue.offset
          }else{
            finalOffset = endOffset
          }
        }
        case "absearliest" =>{
          finalOffset = beginOffset
        }
        case "abslargest" =>{
          finalOffset = endOffset
        }
        case minPattern(m) => {//匹配最近几分钟的offset
          val mpOffset:Long = KafkaMinutePattern.getLastMinsOffset(OffsetKey(topic,partition0.partition(),group),m.toInt)
          if(mpOffset != -1){
            if(mpOffset > beginOffset){
              finalOffset = mpOffset
            }else{
              finalOffset = beginOffset
              logger.warn(s"${partition0.partition()}设置的offset{${offsetValue.offset}低于了offset{$beginOffset}最小值将从头记入,最大offset值是{$endOffset}。")
            }
          }else{
            logger.error("没有找到"+m+"分钟前的记录。")
            KafkaConsumerBasic.consumerRunning.set(false)
            throw new KafkaConsumerException("没有找到"+m+"分钟前的记录。")
            /*if(offsetValue.offset > beginOffset){
              finalOffset = offsetValue.offset
            }*/
          }
        }
        case _:String=>{
          KafkaConsumerBasic.consumerRunning.set(false)
          throw new KafkaConsumerException(s"不支持的mysql_set{$mysql_set}")
        }
      }
      consumer.seek(partition0,finalOffset)
      MysqlKafkaOffset.kafkaOffsetRecord.put(OffsetKey(topic,p.partition(),group),OffsetValue(finalOffset,endOffset-finalOffset,endOffset))
    })
    MysqlKafkaOffset.flush()
    KafkaConsumerRebalanceListener.hasFinishedPartitionsAssigned = true
  }

  override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
      println("ERROR=>onPartitionsRevoked revoked!!!")
  }
}
object KafkaConsumerRebalanceListener{
  def apply( consumer:KafkaConsumer[String, String], topic:String):KafkaConsumerRebalanceListener
   = new KafkaConsumerRebalanceListener(consumer,topic)
  var hasFinishedPartitionsAssigned = false

}