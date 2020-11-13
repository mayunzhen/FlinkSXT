package state

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/12 15:30
  * @version 1.0
  */
object TestSavePoint {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    //2.导入隐式转换
    //3.读取数据
    val stream :DataStream[String]= streamEnv.socketTextStream("bigdata112",8888)
    //4.转换和处理数据
    val result :DataStream[(String,Int)] = stream.flatMap(_.split(" ")).uid("flatmap001")
      .map((_, 1)).uid("map001")
      .setParallelism(2)
      .keyBy(0)
      .sum(1).uid("sum001")
      .setParallelism(2)

    //5.打印结果
    result.print("结果").setParallelism(1)
    //6.启动流计算程序
    streamEnv.execute("wordcount")
  }
}
