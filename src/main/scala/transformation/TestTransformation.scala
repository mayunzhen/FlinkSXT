package transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyCustomerSource
import org.apache.flink.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/11 14:23
  * @version 1.0
  */
object TestTransformation {
  //从自定义的数据源中读取基站日志数据，统计每个基站通话成功的总时长是多少
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream = streamEnv.addSource(new MyCustomerSource)
    //计算
    val result = stream.filter(_.callType.equals("success"))
      .map(log => {
        (log.sid, log.duration)
      }).keyBy(0)
      .reduce((t1, t2) => {
        val duration = t1._2 + t2._2
        (t1._1, duration)
      })
    result.print()
    streamEnv.execute()
  }
}
