package transformation

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import source.StationLog
/**
  * @author mayunzhen
  * @date 2020/11/11 20:02
  * @version 1.0
  * @desc 监控所有手机号码，如果这个号码在5秒内，所有呼叫它的日志都是失败的，则发出告警信息
  *       如果5秒内只要有一个呼叫不是fail失败，则不用告警
  */
object TestProccessFunction {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val dataStream :DataStream[StationLog]= streamEnv.socketTextStream("bigdata112",8888)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })
    //计算
    val result :DataStream[String]= dataStream.keyBy(_.callIn)
      .process(new MonitorCallFail)
    result.print()
    streamEnv.execute()
  }
  class MonitorCallFail extends KeyedProcessFunction[String,StationLog,String]{
    //使用一个状态对象记录时间
    lazy val timeState :ValueState[Long] =  getRuntimeContext.getState(new ValueStateDescriptor[Long]("time",classOf[Long]))

    override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
      //先从状态中获取时间
      var time = timeState.value()
      if(time == 0 && value.callType.equals("failed")){//表示第一次发现呼叫失败，记录当前时间
      //获取当前系统时间 并注册定时器
        var nowTime = ctx.timerService().currentProcessingTime()
        //定时器在5秒后触发
        var onTime = nowTime + 5*1000L
        ctx.timerService().registerProcessingTimeTimer(onTime)
        //把触发的时间保存到状态中
        timeState.update(onTime)
      }
      if(time != 0 && !value.callType.equals("failed")){//表示有一次成功的呼叫，必须要删除定时器
          ctx.timerService().deleteProcessingTimeTimer(time)
        timeState.clear()//清空状态中的时间
      }
    }
    //时间到了，定时器执行
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      var warnStr = "触发的时间："+timestamp+"，手机号："+ctx.getCurrentKey
      out.collect(warnStr)
      timeState.clear()
    }

  }
}
