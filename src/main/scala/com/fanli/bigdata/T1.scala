package com.fanli.bigdata

import java.util.{Date, Timer, TimerTask}

import org.json4s.JValue

/**
  * Created by laichao.wang on 2018/11/19.
  */
object T1 {
  val t = new Thread(new Runnable {
    override def run(): Unit = {
      val timer: Timer = new Timer(true)
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          println("time=>"+new Date()+",block-Size=>")
        }
      },1*1000,3*1000)
    }
  })
  def printInfo={
    val timer: Timer = new Timer()
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        println("time=>"+new Date()+",block-Size=>")
      }
    },1*1000,3*1000)
  }

  case class PartitionOffset(p:Int,o:Long)

  def main(args: Array[String]) {
    import org.json4s.jackson.JsonMethods._
    import org.json4s.DefaultFormats
//    Runtime.getRuntime.addShutdownHook(t)
    implicit val formats = DefaultFormats

    val s = """{"ps":[{"p":5,"o":36},{"p":0,"o":35},{"p":4,"o":36},{"p":2,"o":36},{"p":3,"o":35},{"p":1,"o":35}]}"""
    val json: JValue = parse(s)
    val partitionOffsets: List[PartitionOffset] = (json \ "ps").extract[List[PartitionOffset]]
//    Thread.sleep(1*1000*9)

    val s1= "|1"
    println(s1.split("\\|")(0)=="")
  }
}
