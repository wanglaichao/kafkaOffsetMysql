package com.fanli.bigdata.multiple

import java.util.Properties

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}

/**
  * Created by laichao.wang on 2018/11/28.
  */
object KafkaPropertyBasic {
  /*提供一个连接 供其他类拿取*/
  val props = new Properties()

  private def generateDefaultKafkaProps(): Properties = {
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //默认拉取1000ms
    props.put("kafka.poll.time.out", "500")
    //默认每次拉取条数
    props.put("max.poll.records", "1000")

    // [latest, earliest, none]
    props.put("mysql_offset_reset", "latest")
    props.put("group.id", "latest")
    props
  }


  private  def setKafkaProps(args: Array[String], properties: Properties) = {
    val options: Options = new Options
    EnumKafkaProperty.KafkaProperty.values.foreach(p=>{
      options.addOption(p.toString,true,"")
    })
    EnumKafkaProperty.UnKafkaProperty.values.foreach(p=>{
      options.addOption(p.toString,true,"")
    })

    val parser: DefaultParser = new DefaultParser
    val parse: CommandLine = parser.parse(options,args)

    EnumKafkaProperty.KafkaProperty.values.foreach(p=>{
      if(parse.hasOption(p.toString)){
        properties.setProperty(p.toString.replaceAll("_","."),parse.getOptionValue(p.toString))
      }
    })
    EnumKafkaProperty.UnKafkaProperty.values.foreach(p=>{
      if(parse.hasOption(p.toString)){
        properties.setProperty(p.toString,parse.getOptionValue(p.toString))
      }
    })
    properties
  }

  def verifyKafkaProps(properties: Properties) = {

  }

  def getKafkaProperties(args:Array[String]):Properties={
    val properties:Properties = generateDefaultKafkaProps()
    setKafkaProps(args,properties)
    verifyKafkaProps(properties)
    properties
  }

}
