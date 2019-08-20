package com.fanli.bigdata.until

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

/**
  * Created by laichao.wang on 2018/12/3.
  */
object TimeUtil {
  def getLastMins(m: Int): String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(new Date)
    calendar.add(Calendar.MINUTE,-1*m)
    format.format(calendar.getTime)
  }

  def main(args: Array[String]) {

    println(getLastMins(15))
  }

}
