package sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/**
  * @author mayunzhen
  * @date 2020/11/11 10:28
  * @version 1.0
  *          @desc 把netcat作为数据源，并且统计每个单词次数，统计的结果写入Redis数据库中
  */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream :DataStream[String] = streamEnv.socketTextStream("bigdata112",8888)
    val result :DataStream[(String,Int)] = stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val config :FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(0)
      .setHost("183.213.26.107")
      .setPort(6379)
      .setPassword("test1234")
      .build()

    result.addSink(new RedisSink[(String,Int)](config,new RedisMapper[(String, Int)] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"t_wc")
      }

      override def getKeyFromData(t: (String, Int)): String = {
        t._1
      }

      override def getValueFromData(t: (String, Int)): String = {
        t._2+""
      }
    }))
    streamEnv.execute()
  }
}
