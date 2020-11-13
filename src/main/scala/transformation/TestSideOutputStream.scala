package transformation

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import source.StationLog
import transformation.TestSideOutputStream.notSuccessTag
/**
  * @author mayunzhen
  * @date 2020/11/12 8:37
  * @version 1.0
  *          @desc 把呼叫成功的数据输出到主流，不成功的数据输出到侧流
  */
object TestSideOutputStream {
  var notSuccessTag = new OutputTag[StationLog]("not_success")//不成功侧流的标签
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    var filePath = getClass.getResource("/station.log").getPath
    val stream :DataStream[StationLog] = streamEnv.readTextFile(filePath)
      .map(line=>{
        var arr = line.split(",")
        new StationLog(arr(0).trim(),arr(1).trim(),arr(2).trim(),arr(3).trim(),arr(4).trim().toLong,arr(5).trim().toLong)
      })
    //
    val result :DataStream[StationLog] = stream.process(new CreateSideOutPutStream(notSuccessTag))
    result.print()//主流

    //根据主流获取侧流
    val sideStream = result.getSideOutput(notSuccessTag)
    sideStream.print()

    streamEnv.execute()
  }
  class CreateSideOutPutStream(tag :OutputTag[StationLog]) extends ProcessFunction[StationLog,StationLog]{
    override def processElement(value: StationLog, ctx: ProcessFunction[StationLog, StationLog]#Context, out: Collector[StationLog]): Unit = {
      if (value.callType.equals("success")){
        //输出到主流
        out.collect(value)
      }else{//输出到侧流
        ctx.output(notSuccessTag,value)
      }
    }
  }
}
