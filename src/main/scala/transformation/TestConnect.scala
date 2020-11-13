package transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
/**
  * @author mayunzhen
  * @date 2020/11/11 14:37
  * @version 1.0
  */
object TestConnect {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream1:DataStream[(String,Int)] = streamEnv.fromElements(("a",1),("b",2),("c",3))
    val stream2 :DataStream[String] = streamEnv.fromElements("e","f","g")
    val stream3 :ConnectedStreams[(String,Int),String] = stream1.connect(stream2)//中间产品
    //使用coMap,coFlatMap
    val result:DataStream[(String,Int)] = stream3.map(
      //第一个处理函数
      t => {
        (t._1, t._2)
      },
      //第一个处理函数
      t => {
        (t, 0)
      }
    )
    result.print()
    streamEnv.execute()
  }
}
