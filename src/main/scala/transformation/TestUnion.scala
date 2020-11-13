package transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import source.MyCustomerSource
import org.apache.flink.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/11 14:31
  * @version 1.0
  */
object TestUnion {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream1 = streamEnv.fromElements(("a",1),("b",2))
    val stream2 = streamEnv.fromElements(("b",5),("d",6))
    val stream3 = streamEnv.fromElements(("e",7),("f",8))
    val result = stream1.union(stream2,stream3)
    result.print()
    streamEnv.execute()
  }
}
