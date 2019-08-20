package com.fanli.bigdata.multiple

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import com.fanli.bigdata.db.C3p0DbPool
import com.fanli.bigdata.until.TimeUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by laichao.wang on 2018/11/29.
  */
object MysqlKafkaOffset {
  val logger:Logger = LoggerFactory.getLogger(this.getClass)
  var dbPropryName=""

  //从数据库中拉取最近几分钟前的一条数据
  def loadLastMinsOffsetFromMysql(topic: String, group: String,  m: Int)
  : mutable.Map[OffsetKey, Long] = {
    val map = mutable.Map[OffsetKey, Long]()
    val manager: C3p0DbPool = C3p0DbPool.getMDBManager(dbPropryName)
    val connection =  manager.getConnection()
    val sql = "select partitions_info from kafka_consumer_group_offset_records " +
      "  where topic=? and group_id=? and offset_time>=? limit 1"
    val prepareStatement: PreparedStatement = connection.prepareStatement(sql)
    prepareStatement.setString(1,topic)
    prepareStatement.setString(2,group)
    prepareStatement.setString(3,TimeUtil.getLastMins(m))
    val resultSet: ResultSet = prepareStatement.executeQuery()
    if (resultSet.next()){

      val  partitionInfo = resultSet.getString("partitions_info")
      logger.info(s"load $m partitions info $partitionInfo")
      import org.json4s.jackson.JsonMethods._
      import org.json4s.DefaultFormats
      implicit val formats = DefaultFormats
      val json = parse(partitionInfo)
      val partitionOffsets: List[PartitionOffset] = (json \ "ps").extract[List[PartitionOffset]]
      partitionOffsets.foreach(po=>{
        map +=(OffsetKey(topic,po.p,group)->po.o)
      })
    }
    close(connection,prepareStatement,resultSet)
    map
  }


  val kafkaOffsetRecord =  scala.collection.mutable.Map[OffsetKey,OffsetValue]()

  def initOffset(topic: String, group: String) = {
    val manager: C3p0DbPool = C3p0DbPool.getMDBManager(dbPropryName)
    val connection =  manager.getConnection()
    val sql = "select * from kafka_consumer_group_offset where topic = ? and group_id = ?"
    val prepareStatement: PreparedStatement = connection.prepareStatement(sql)
    prepareStatement.setString(1,topic)
    prepareStatement.setString(2,group)
    val resultSet: ResultSet = prepareStatement.executeQuery()
    while (resultSet.next()){
      val  partition = resultSet.getInt("partition")
      val offset: Long = resultSet.getLong("offset")
      val log_size: Long = resultSet.getLong("log_size")
      val lag: Long = resultSet.getLong("lag")
      MysqlKafkaOffset.kafkaOffsetRecord.put(OffsetKey(topic,partition,group),OffsetValue(offset,lag,log_size))
    }
    close(connection,prepareStatement,resultSet)
  }

  //刷新offset 到数据库
  def flush() = {
    val manager: C3p0DbPool = C3p0DbPool.getMDBManager(dbPropryName)
    val connection = manager.getConnection()
    connection.setAutoCommit(false)
    val sql = s"""INSERT INTO kafka_consumer_group_offset
              (topic,`partition`,group_id,`offset`,lag,log_size,update_time)
              VALUES (?,?,?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE `offset`=?,lag=?,log_size=?,update_time=now()""".stripMargin
    val prepareStatement: PreparedStatement = connection.prepareStatement(sql)
    kafkaOffsetRecord.foreach(p=>{
       prepareStatement.setString(1,p._1.topic)
       prepareStatement.setInt(2,p._1.partition)
       prepareStatement.setString(3,p._1.group)

       prepareStatement.setLong(4,p._2.offset)
       prepareStatement.setLong(5,p._2.lag)
       prepareStatement.setLong(6,p._2.logSize)
       prepareStatement.setLong(7,p._2.offset)
       prepareStatement.setLong(8,p._2.lag)
       prepareStatement.setLong(9,p._2.logSize)
       prepareStatement.addBatch()
    })
    prepareStatement.executeBatch()
    connection.commit()
    close(connection,prepareStatement,null)
  }

  def close(connection:Connection,statment:Statement,resultSet: ResultSet): Unit ={
    if(resultSet != null){
      resultSet.close()
    }
    if(statment != null){
      statment.close()
    }
    if ( connection != null){
      connection.close()
    }

  }

  @throws[Exception]
  def flushPartitionsOffset2db(): Unit = {
    if(kafkaOffsetRecord.size>0){
      var topic = ""
      var group = ""
      //只针对一个topic的情景
      val ps = kafkaOffsetRecord.map(m=>{
        topic = m._1.topic
        group = m._1.group
        val p = m._1.partition
        val offset = m._2.offset
//        val lag = m._2.lag
//        val logSize = m._2.logSize
//        s"{'p':$p,'o':$offset,'lg':$lag,'ls':$logSize}"
        s"""{"p":$p,"o":$offset}"""
      }).mkString(",")
      val ps_info = s"""{"ps":[$ps]}"""
      val manager: C3p0DbPool = C3p0DbPool.getMDBManager(dbPropryName)
      val connection = manager.getConnection()
      val sql = "insert into kafka_consumer_group_offset_records(topic,group_id,partitions_info,offset_time) values" +
        " (?,?,?,now()) "
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1,topic)
      statement.setString(2,group)
      statement.setString(3,ps_info)
      statement.execute()
      close(connection,statement,null)
    }
  }


}

case class OffsetKey(topic:String,partition:Int,group: String)
case class OffsetValue(offset:Long,lag:Long,logSize:Long)
case class PartitionOffset(p:Int,o:Long)
