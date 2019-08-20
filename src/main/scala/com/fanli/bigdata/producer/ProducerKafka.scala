package com.fanli.bigdata.producer

import java.util.Properties

import kafka.producer.Producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by laichao.wang on 2018/12/4.
  */
object ProducerKafka {

  val arr:Array[String] = Array[String](
    "110.179.31.60 1273152592.194018721.400474258 - [01/Jan/2019:00:00:00 +0800] \"GET /index.html?evttype=cd&spm=super_apply_bbs.pc.pty-display~index-1~acid-293292&utmo=1273152592.194018721.400474258&utmp=2382179670.1835252580.3734069722&userid=220723200&tid=ECE04E0D-B338-4649-AB0D-C5AC45E9781E&ptid=30AE0EEC-D0AA-47B7-B735-D4ED3BDD32C7&timestamp=1546272001322&flpn=superapp_apply_index_index HTTP/1.1\" 200 0 \"https://super.fanli.com/apply/index\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134\" 0.000 220723200 ubt2.fanli.com 192.168.3.56 192.168.2.232 2382179670.1835252580.3734069722 - -",
    "10.5.31.60 1273152592.194018721.400474258 - [01/Jan/2019:00:00:00 +0800] \"GET /index.html?evttype=cd&spm=super_apply_bbs.pc.pty-display~index-1~acid-293292&utmo=1273152592.194018721.400474258&utmp=2382179670.1835252580.3734069722&userid=220723200&tid=ECE04E0D-B338-4649-AB0D-C5AC45E9781E&ptid=30AE0EEC-D0AA-47B7-B735-D4ED3BDD32C7&timestamp=1546272001322&flpn=superapp_apply_index_index HTTP/1.1\" 200 0 \"https://super.fanli.com/apply/index\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134\" 0.000 220723200 ubt2.fanli.com 192.168.3.56 192.168.2.232 2382179670.1835252580.3734069722 - -"
  )


  def main(args: Array[String]) {
    val props:Properties = new Properties();
    props.put("bootstrap.servers", "10.0.5.165:9092,10.0.5.166:9092");
    props.put("acks", "all");
    props.put("retries", "0");
    props.put("batch.size", "16384");
    props.put("linger.ms", "1");
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val size: Int = arr.size
    val producer= new KafkaProducer[String, String](props);
    for (i <- 1 to  Int.MaxValue){
      val s:String = arr( (i % size))
      producer.send(new ProducerRecord[String, String]("apache_topic", i.toString, s));
      Thread.sleep(200)
    }
    producer.close()
  }
}
