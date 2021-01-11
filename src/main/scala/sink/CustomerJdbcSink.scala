package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import source.{MyCustomerSource, StationLog}
/**
  * @author mayunzhen
  * @date 2020/11/11 13:30
  * @version 1.0
  * @Desc 随机生成StationLog，push到Mysql
  */
object CustomerJdbcSink {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream = streamEnv.addSource(new MyCustomerSource)
    //数据写入Mysql，需要创建自定义的sink
    stream.addSink(new MyCustomerJdbcSink)
    streamEnv.execute()
  }

  /**
    * 自定义Sink类
    */
  class MyCustomerJdbcSink extends RichSinkFunction[StationLog]{
    var conn :Connection = _
    var pst : PreparedStatement = _
    //把StationLog对象写入Mysql表中，每写入一条执行一次
    override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
      pst.setString(1,value.sid)
      pst.setString(2,value.callOut)
      pst.setString(3,value.callIn)
      pst.setString(4,value.callType)
      pst.setLong(5,value.callTime)
      pst.setLong(6,value.duration)
      pst.executeUpdate()
  }
  //Sink初始化的时候调用一次，一个并行度初始化一次
    //创建连接对象，和Statement对象
  override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","test1234")
      pst = conn.prepareStatement("insert into t_station_log(sid,call_out,call_in,call_type,call_time,duration) values(?,?,?,?,?,?)")
  }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
}
}
