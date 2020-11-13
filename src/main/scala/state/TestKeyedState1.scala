package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import source.StationLog
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
/**
  * @author mayunzhen
  * @date 2020/11/12 9:10
  * @version 1.0
  * @desc 统计每个手机的呼叫时间间隔，单位是毫秒
  */
object TestKeyedState1 {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    var filePath = getClass.getResource("/station.log").getPath
    val stream :DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })
    val result :DataStream[(String, Long)] = stream.keyBy(_.callOut)
      .flatMap(new CallIntervalFunction)
    result.print()
    streamEnv.execute()
  }
  //输出是一个二元组(手机号，时间间隔)，输入是StationLog
  class CallIntervalFunction extends RichFlatMapFunction[StationLog,(String,Long)]{
    //定义一个状态，用于保存前一次呼叫的时间
    private var preCallTimeState : ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      preCallTimeState = getRuntimeContext.getState(new ValueStateDescriptor[Long] ("pre",classOf[Long]))
    }
    override def flatMap(value: StationLog, out: Collector[(String, Long)]): Unit = {
      //从状态中获取前一次呼叫的时间
      var preCallTime = preCallTimeState.value()
      if(preCallTime == null || preCallTime == 0){//状态中没有，代表第一次呼叫
        preCallTimeState.update(value.callTime)
      }else{
        //状态中有数据，则计算时间间隔
        var interval = value.callTime - preCallTime
        out.collect(value.callOut,interval)
      }
    }
  }
}
