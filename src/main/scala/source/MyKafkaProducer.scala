package source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * @author mayunzhen
  * @date 2020/11/10 19:37
  * @version 1.0
  */
object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","183.213.26.120:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)
    var producer = new KafkaProducer[String,String](props);
    var r = new Random()
    while (true){
      val data = new ProducerRecord[String,String]("test_bsm","key"+r.nextInt(10))
      producer.send(data)
      Thread.sleep(1000)
    }
    producer.close()
  }
}
