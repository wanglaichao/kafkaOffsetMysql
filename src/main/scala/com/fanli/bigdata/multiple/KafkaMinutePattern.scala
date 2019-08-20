package com.fanli.bigdata.multiple

import org.apache.kafka.common.TopicPartition

/**
  * Created by laichao.wang on 2018/12/3.
  */
object KafkaMinutePattern {
  private var  map:scala.collection.mutable.Map[OffsetKey,Long] = null

  def getLastMinsOffset(offsetKey: OffsetKey, m: Int): Long = {
    if ( map == null){
      map = MysqlKafkaOffset.loadLastMinsOffsetFromMysql(offsetKey.topic,offsetKey.group,m)
    }
    map.get(offsetKey).getOrElse(-1)
  }




}
