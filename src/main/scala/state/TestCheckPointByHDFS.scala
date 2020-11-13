package state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/12 15:13
  * @version 1.0
  *          @Desc 使用wordCount测试一下HDFS的状态后端，先运行一段时间Job，然后cancel，
  *               再重新启动，看下状态是否连续
  */
object TestCheckPointByHDFS {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //开启checkpoint 并设置一些参数
    streamEnv.enableCheckpointing(5000)//每隔5秒开启一次checkpoint
    //存放检查点数据地址
    streamEnv.setStateBackend(new FsStateBackend("hdfs://183.213.26.120:9000/checkpoint/cp1"))
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(5000)
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //2.导入隐式转换
    //3.读取数据
    val stream :DataStream[String]= streamEnv.socketTextStream("bigdata112",8888)
    //4.转换和处理数据
    val result :DataStream[(String,Int)] = stream.flatMap(_.split(" "))
      .map((_, 1)).setParallelism(2)
      .keyBy(0)
      .sum(1).setParallelism(2)

    //5.打印结果
    result.print("结果").setParallelism(1)
    //6.启动流计算程序
    streamEnv.execute("wordcount")
  }
}
