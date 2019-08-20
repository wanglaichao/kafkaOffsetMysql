package com.fanli.bigdata.test

import sun.misc.{Signal, SignalHandler}

/**
  * Created by laichao.wang on 2018/12/3.
  */
object SingalDemo1 {

  //handler = new ShutdownHandler();
//  Signal.handle(new Signal("TERM"), handler); // 相当于kill -15
//  Signal.handle(new Signal("INT"), handler); // 相当于Ctrl+C
  def main(args: Array[String]) {
    val singalDemo: SingalDemo1 = new SingalDemo1()



  }

}

class SingalDemo1  extends SignalHandler {
  override def handle(signal: Signal): Unit = {


  }

}
