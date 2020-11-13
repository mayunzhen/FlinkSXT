//package source
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.flink.streaming.api.scala._
///**
//  * @author mayunzhen
//  * @date 2020/11/10 19:20
//  * @version 1.0
//  */
//object KafkaSourceByKeyValue {
//  def main(args: Array[String]): Unit = {
//    //1.初始化流计算环境
//    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnv.setParallelism(1)
//    //2.导入隐式转换
//
//    //3.连接kafka
//    val props = new Properties()
//    props.setProperty("bootstrap.servers","183.213.26.120:9092")
//    props.setProperty("group.id","fink01")
//    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
//    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
//    props.setProperty("auto.offset.reset","latest") //设置kafka为数据源
//    val stream = streamEnv.addSource(new FlinkKafkaConsumer[(String,String)]("test_bsm",new MyKafkaReader(),props))
//    stream.print()
//    streamEnv.execute()
//  }
//  //自定义类 从Kafka中读取键值对数据
//  class MyKafkaReader extends DeserializationSchema[(String,String)]{
//  //反序列化
//    override def deserialize(record: ConsumerRecord[ Array[Byte], Array[Byte]]): (String, String) = {
//      if(record != null){
//        var key = "null"
//        var value = "null"
//        if(record.key() != null){
//          key = new String(record.key(),"UTF-8")
//        }
//        if(record.value() != null){
//          value = new String(record.key(),"UTF-8")
//        }
//        (key,value)
//      }else{//数据为空
//        ("null","null")
//      }
//    }
//    //是否流结束
//    override def isEndOfStream(nextElement: (String, String)): Boolean = {
//      false
//    }
//  //指定类型
//    override def getProducedType: TypeInformation[(String, String)] = {
//      createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])
//    }
//  }
//}
