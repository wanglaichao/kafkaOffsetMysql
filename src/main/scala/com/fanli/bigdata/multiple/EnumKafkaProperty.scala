package com.fanli.bigdata.multiple


/**
  * Created by laichao.wang on 2018/11/28.
  */
object EnumKafkaProperty {
  object KafkaProperty extends Enumeration{
    type  KafkaProperty = Value
    val KAFKA_BOOTSTRAP_SERVERS =Value("bootstrap_servers")
    val KAFKA_ENABLE_AUTO_COMMIT = Value("enable_auto_commit")
    //consumer api 一次从kafka拉取的时常
    val KAFKA_KAFKA_POLL_TIME_OUT	 = Value("kafka_poll_time_out")
    val KAFKA_MAX_POLL_RECORDS		 = Value("max_poll_records")

  }
  object UnKafkaProperty extends Enumeration{
    type  UnKafkaProperty = Value
    val UNKAFKA_GROUP = Value("group")
    val UNKAFKA_TOPIC = Value("topic")
    val UNKAFKA_MYSQL_OFFSET_RESET	 = Value("mysql_offset_reset")

  }


  def main(args: Array[String]) {
    KafkaProperty.values.foreach(v=>{
      println(v)
    })
    println(KafkaProperty.KAFKA_BOOTSTRAP_SERVERS)
  }
}
