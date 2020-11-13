//package sink
//import java.util.Properties
//
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
//import org.apache.flink.api.common.serialization.SerializationSchema
//import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
//import org.apache.kafka.clients.producer.ProducerRecord
///**
//  * @author mayunzhen
//  * @date 2020/11/11 12:17
//  * @version 1.0
//  */
//object KafkaSinkByKeyValue {
//  def main(args: Array[String]): Unit = {
//    val props = new Properties()
//    props.setProperty("bootstrap.servers","183.213.26.120:9092")
//
//    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnv.setParallelism(1)
//    val stream :DataStream[String] = streamEnv.socketTextStream("bigdata112",8888)
//    val result :DataStream[(String,Int)] = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
//
//    val value = new FlinkKafkaProducer[(String, Int)]("t_2020", new KeyedSerializationSchema[(String, Int)] {
//      override def serialize(element: (String, Int)): Array[Byte] = {
//       val array = Array[Byte](3)
//        array
//      }
//    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
////    new FlinkKafkaProducer[(String,Int)]("t_2020",new SerializationSchema[(String, Int)] {
////      override def serialize(element: (String, Int),timeStamp : Long): Array[Byte] = {
////        new ProducerRecord("t_2020",element._1.getBytes(),(element._2+"").getBytes())
////      }
////    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
//    streamEnv.execute()
//  }
//}
