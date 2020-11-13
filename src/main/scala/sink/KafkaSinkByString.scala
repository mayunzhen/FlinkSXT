package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.scala._
/**
  * @author mayunzhen
  * @date 2020/11/11 11:28
  * @version 1.0
  */
object KafkaSinkByString {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream :DataStream[String] = streamEnv.socketTextStream("bigdata112",8888)
    val words :DataStream[String] = stream.flatMap(_.split(" "))

    words.addSink(new FlinkKafkaProducer[String]("183.213.26.120:9092","t_2020",new SimpleStringSchema()))
    streamEnv.execute()
  }
}
