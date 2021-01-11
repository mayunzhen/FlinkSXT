package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import source.StationLog

/**
  * @author mayunzhen
  * @date 2020/11/12 9:10
  * @version 1.0
  * @desc 第二种方法 统计每个手机的呼叫时间间隔，单位是毫秒
  */
object TestKeyedState2 {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    var filePath = getClass.getResource("/station.log").getPath
    val stream :DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })
    val result :DataStream[(String, Long)] = stream.keyBy(_.callOut)//分组
      //有两种情况，1.状态中有上一次的通话时间，2.没有，采用Scala中的模式匹配
      .mapWithState[(String,Long),StationLog]{
      case (in:StationLog,None)=>((in.callOut,0),Some(in))//状态中没有值
      case (in:StationLog,pre:Some[StationLog])=>{
        var interval = in.callTime - pre.get.callTime
        ((in.callOut,interval),Some(in))
      }
    }.filter(_._2 !=0)
    result.print()
    streamEnv.execute()
  }

}
