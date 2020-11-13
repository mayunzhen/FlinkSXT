package source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.util.Random

/**
  * @author mayunzhen
  * @date 2020/11/10 20:21
  * @version 1.0
  */
/**
  * 自定义Source，每隔
  */
class MyCustomerSource extends SourceFunction[StationLog]{
  //是否中止数据流
  var flag = true;
  /*
  主要的方法，启动一个Source，并且从Source中返回数据
  如果run方法停止，则数据流中止
   */
  override def run(ctx: SourceFunction.SourceContext[StationLog]): Unit = {
    var types = Array("fail","busy","barring","success")
    val r = new Random()
    while (flag){
      1.to(10).map(i=>{
        var callOut = "1860000%04d".format(r.nextInt(10000))//主叫号码
        var callIn = "1890000%04d".format(r.nextInt(10000))//被叫号码
        //生成一条数据
        new StationLog("station_"+r.nextInt(10),callOut,callIn,types(r.nextInt(4)),System.currentTimeMillis(),r.nextInt(100))
      }).foreach(ctx.collect(_))//发送数据到流
    Thread.sleep(2000)//每隔两秒执行一次
    }
  }

  //中止数据流
  override def cancel(): Unit = {
    flag = false;
  }
}
object CustomerSource {
  def main(args: Array[String]): Unit = {
    val streamEnv :StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream = streamEnv.addSource(new MyCustomerSource)
    stream.print()
    streamEnv.execute()
  }
}
