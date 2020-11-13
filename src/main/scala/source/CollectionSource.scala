package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author mayunzhen
  * @date 2020/11/10 19:00
  * @version 1.0
  */
/**
  * 基站日志
  * @param sid 基站id
  * @param callOut 主叫号码
  * @param callIn 被叫号码
  * @param callType 呼叫类型
  * @param callTime 呼叫时间(毫秒)
  * @param duration 通话时长(秒)
  */
case class StationLog(
                     sid:String,
                     var callOut:String,
                     var callIn:String,
                     var callType:String,
                     var callTime:Long,
                     var duration:Long
                     )

object CollectionSource {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    var stream:DataStream[StationLog]=streamEnv.fromCollection(Array(
      new StationLog("001","1866","189","busy",System.currentTimeMillis(),0),
    new StationLog("002","1866","188","busy",System.currentTimeMillis(),0),
    new StationLog("003","1866","183","busy",System.currentTimeMillis(),0),
    new StationLog("004","1866","186","success",System.currentTimeMillis(),20),
    new StationLog("005","1866","189","busy",System.currentTimeMillis(),0),
    new StationLog("006","1866","189","busy",System.currentTimeMillis(),0)
    ))
    stream.print()
    streamEnv.execute()
  }
}
