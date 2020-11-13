package source

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author mayunzhen
  * @date 2020/11/10 19:13
  * @version 1.0
  */
object KafkaSource1 {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算环境
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3.连接kafka
    val props = new Properties()
    props.setProperty("bootstrap.servers","183.213.26.120:9092")
    props.setProperty("group.id","fink01")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest") //设置kafka为数据源
    val stream = streamEnv.addSource(new FlinkKafkaConsumer[String]("test_bsm",new SimpleStringSchema(),props))
    stream.print()
    streamEnv.execute()
  }
}
