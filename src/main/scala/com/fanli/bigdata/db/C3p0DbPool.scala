package com.fanli.bigdata.db

import java.sql.{Connection, SQLException, Statement}
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import kafka.utils.Logging

/**
  * Created by laichao.wang on 2017/3/6.
  */
class C3p0DbPool(val proFileName: String, val host: String, val port: String, val db: String,val initPool:Int,val minPool:Int,val maxPool:Int,val dbType:String = "mysql")
  extends  Serializable{

  val proFile = if (proFileName == null) "nine_mysql.properties" else proFileName
  val prop = new Properties()
  prop.load(this.getClass.getResourceAsStream("/" + proFile))
  if(!prop.containsKey("url")){
    var url = ""
    if(dbType ==  "sqlserver"){
      url = "jdbc:sqlserver://" + host + ";databaseName=" + db
    }else{
      url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?characterEncoding=UTF8&autoReconnect=true&rewriteBatchedStatements=true"
    }
    prop.setProperty("url", url)
  }

  /**
    * 构造参数
    *
    * @param proFileName
    * @return
    */
  def this(proFileName: String ) = this( proFileName,  "", "",  "", 5,5,10,"")

  def this(proFileName: String,dbType:String ) = this( proFileName,  "", "",  "", 5,5,10,dbType)
  def this(proFileName: String,  initPool:Int, minPool:Int, maxPool:Int ) = this( proFileName,  "", "",  "", initPool,minPool, maxPool)
  def this(proFileName: String,  initPool:Int, minPool:Int, maxPool:Int,dbType:String  ) = this( proFileName,  "", "",  "", initPool,minPool, maxPool,dbType)
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true);
  cpds.setJdbcUrl(prop.getProperty("url").toString());
  cpds.setDriverClass(prop.getProperty("driverClassName").toString());
  cpds.setUser(prop.getProperty("username").toString());
  cpds.setPassword(prop.getProperty("password").toString());
  cpds.setAcquireIncrement(1)
  cpds.setMinPoolSize(minPool)
  cpds.setMaxPoolSize(maxPool)
  cpds.setInitialPoolSize(initPool)
  cpds.setCheckoutTimeout(60000)
  cpds.setMaxIdleTime(3600)
  cpds.setIdleConnectionTestPeriod(14400)

  println("^^^^^^^^^^^^"+prop.getProperty("driverClassName")+"#"+prop.getProperty("url")+"#"+prop.getProperty("username").toString()+"#******")

  def getDataSource:ComboPooledDataSource=cpds

  @throws[SQLException]
  def getConnection():Connection  = {
    synchronized{
//      logError("getConnection##1###"+cpds)
      val connection: Connection = cpds.getConnection()
      connection.setAutoCommit(true)
      connection
    }
  }
  @throws[SQLException]
  def closeConn(conn: Connection,st:Statement): Unit = {
      if (st != null) {
        st.close()
      }
      if (conn != null && !conn.isClosed) {
        conn.setAutoCommit(true)
        conn.close()
      }
  }

}

object C3p0DbPool extends Logging{
  var mdbManager:C3p0DbPool=_

  @throws[SQLException]
  def getMDBManager(proFileName: String, host: String, port: String, db: String,initPool:Int, minPool:Int, maxPool:Int,dbType: String):C3p0DbPool={
    synchronized{
      if(mdbManager==null){
        println("C3p0DbPool@@@@"+proFileName+"#"+host+"#"+port+"#"+db+"#"+dbType)
        mdbManager = new C3p0DbPool(proFileName, host, port, db,initPool,minPool,maxPool , dbType)
      }
    }
    mdbManager
  }
  @throws[SQLException]
  def getMDBManager(proFileName: String):C3p0DbPool={
    synchronized{
      if(mdbManager==null){
        println("C3p0DbPool@@@@"+proFileName)
        mdbManager = new C3p0DbPool(proFileName)
      }
    }
    mdbManager
  }
  @throws[SQLException]
  def getMDBManager(proFileName: String,dbType: String):C3p0DbPool={
    synchronized{
      if(mdbManager==null){
        println("C3p0DbPool@@@@"+proFileName)
        mdbManager = new C3p0DbPool(proFileName)
      }
    }
    mdbManager
  }
  @throws[SQLException]
  def getMDBManager(proFileName: String,initPool:Int, minPool:Int, maxPool:Int):C3p0DbPool={
    synchronized{
      if(mdbManager==null){
        println("C3p0DbPool@@@@"+proFileName)
        mdbManager = new C3p0DbPool(proFileName, initPool,minPool,maxPool)
      }
    }
    mdbManager
  }
  @throws[SQLException]
  def getMDBManager(proFileName: String,initPool:Int, minPool:Int, maxPool:Int,dbType: String):C3p0DbPool={
    synchronized{
      if(mdbManager==null){
        println("C3p0DbPool@@@@"+proFileName)
        mdbManager = new C3p0DbPool(proFileName, initPool,minPool,maxPool,dbType)
      }
    }
    mdbManager
  }
}