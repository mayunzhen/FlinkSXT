package transformation

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import source.{MyCustomerSource, StationLog}
import org.apache.flink.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/11 16:37
  * @version 1.0
  */
object TestFunctionClass {
  //计算出每个通话成功的日志中的通话时长（起始和结束时间）,并且按照指定格式输出时间
  //数据源来自本地文件
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    var filePath = getClass.getResource("/station.log").getPath
    val stream :DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })

    //定义一个时间格式
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val result :DataStream[String]= stream.filter(_.callType.equals("success"))
      .map(new MyMapFunction(sdf))
    result.print()
    streamEnv.execute()
  }
  class  MyMapFunction(format: SimpleDateFormat) extends MapFunction[StationLog,String]{
    override def map(value: StationLog): String = {
      var startTime = value.callTime;
      var endTime = startTime + value.duration*1000;
      "主叫号码："+value.callOut+",被叫号码："+value.callIn+",呼叫起始时间："+format.format(new Date(startTime))+",呼叫结束时间："+format.format(new Date(endTime))
    }
  }
}
