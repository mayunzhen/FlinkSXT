package transformation

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}
import source.{MyCustomerSource, StationLog}
import org.apache.flink.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/11 14:47
  * @version 1.0
  */
object TestSplitAndSelect {
  //从自定义数据源中读取基站通话日志，把通话成功的和通话失败的分离出来
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
   val stream :DataStream[StationLog]= streamEnv.addSource(new MyCustomerSource)
    //数据源切割
    val splitStream : SplitStream[StationLog] = stream.split(log => {
      //给两个不同条件的数据打上不同的数据标签
      if (log.callType.equals("success"))
        Seq("Success")
      else
        Seq("No Success")
    })
    val stream1 = splitStream.select("Success")//根据标签得到不同的数据流
    val stream2 = splitStream.select("No Success")
//    stream1.print("通话成功")
    stream2.print("通话不成功")
    streamEnv.execute()
  }
}
