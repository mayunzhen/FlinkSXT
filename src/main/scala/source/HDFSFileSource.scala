package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author mayunzhen
  * @date 2020/11/10 18:52
  * @version 1.0
  */
object HDFSFileSource {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val stream :DataStream[String] = streamEnv.readTextFile("hdfs://hadoop101:9000/wc.txt")
    //单词统计计算
    val result :DataStream[(String, Int)]= stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    result.print()
    streamEnv.execute()
  }
}
