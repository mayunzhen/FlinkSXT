package transformation

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import source.StationLog
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
/**
  * @author mayunzhen
  * @date 2020/11/11 19:15
  * @version 1.0
  * @Desc 把通话成功的电话号码转换成真实用户姓名，用户姓名保存在MySql中
  */
object TestRichFunctionClass {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    var filePath = getClass.getResource("/station.log").getPath
    val stream :DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })
    //计算:把电话号码变成用户姓名
    val result :DataStream[StationLog]= stream.filter(_.callType.equals("success"))
      .map(new MyRichMapFunction)
    result.print()
    streamEnv.execute("马云珍测试富函数")
  }
  //自定义一个富函数类
  class MyRichMapFunction extends RichMapFunction[StationLog,StationLog]{
    var conn :Connection = _
    var pst :PreparedStatement = _
    override def map(value: StationLog): StationLog = {
      print(getRuntimeContext.getTaskName)
      //查询主叫号码对应的姓名
      pst.setString(1,value.callOut)
      var result :ResultSet = pst.executeQuery()
      if(result.next()){
        value.callOut = result.getString(1)
      }
      //查询被叫号码对应的姓名
      pst.setString(1,value.callIn)
       result = pst.executeQuery()
      if(result.next()){
        value.callIn = result.getString(1)
      }
      value
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","test1234")
      pst = conn.prepareStatement("select name from t_phone where phone_number=?")
    }
  }
}
