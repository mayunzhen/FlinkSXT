import java.net.URL

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/10 14:51
  * @version 1.0
  * @desc 批处理计算
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1.初始化批处理计算环境
    val env :ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataPath :URL = getClass.getResource("/wc.txt")
    val data : DataSet[String] = env.readTextFile(dataPath.getPath)//DataSet == spark RDD
    //计算
    data.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

  }
}
