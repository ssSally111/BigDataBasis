package flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

// (名称,性别,年龄)
case class Person(name: String, sex: String, age: Int)

// (id,时间戳,温度)
case class SensorReading(id: String, date: Long, data: Double)

object Text03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO: 流处理API_source

    // 1.从集合中读取数据流
    val dataList = List(
      Person("李四", "男", 185),
      Person("张三", "男", 182),
      Person("小明", "男", 178),
      Person("王五", "男", 175),
      Person("小红", "女", 173)
    )
    val stream1 = env.fromCollection(dataList)
    //    env.fromElements(32,0.25,"jtyj")


    // 2.从文件读取数据
    val stream2 = env.readTextFile("src/main/resources/datas/hotelorder.csv")


    // 3.从kafka中读取数据  (在172.29.44.241中启动zk+kafka,运行 bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor ,如果输入数据报警告无法连接请检查配置文件取消listeners,advertised.listeners注释并配置自己hostname)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.29.44.241:9092")
    properties.setProperty("group.id", "consumer-group")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // 4.自定义source
    val stream4 = env.addSource(new MySensorSource())


//    stream1.print()
//    stream2.print()
//    stream3.print()
//    stream4.print().setParallelism(1) // 设置cpu调度为1,保证数据有序输出

    env.execute("text_source")
  }
}

// 自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading] {
  // 定义一个标识符flag,表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 随机生成数据(id,data)
    val rand = new scala.util.Random()
    var curTemp = 1.to(10).map(i => (s"sensor_${i}", rand.nextDouble() * 100))

    // 不停产生产数据，除非被cancel
    while (running) {
      // 微调数据
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取时间戳
      val curTime = System.currentTimeMillis()

      // 加入当前时间戳,调用sourceContext.collect发出数据
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔500ms
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = running = false
}