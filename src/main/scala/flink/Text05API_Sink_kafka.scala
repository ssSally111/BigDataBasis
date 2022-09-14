package flink

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object Text05API_Sink_kafka {
  def main(args: Array[String]): Unit = {
    // TODO: API_Sink_kafka

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)

    // 读数据
        val inputStream = env.readTextFile("src/main/resources/datas/text.txt")
        val dataStream = inputStream
          .map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
          })


    //    1.写到本地

        dataStream.writeAsCsv("/home/icetea/project/spark/demo0317/src/main/resources/datas/sink-test.txt")
//        dataStream.addSink(StreamingFileSink.forRowFormat(
//          new Path("/home/icetea/project/spark/demo0317/src/main/resources/datas/sink-test.txt"),
//          new SimpleStringEncoder[SensorReading]()
//        ).build())


    // 2.flink-生产者 到 kafka-消费者

    // 在172.29.44.241中运行./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest
    //    dataStream.addSink(new FlinkKafkaProducer011[String]("172.29.44.241:9092", "sinktest", new SimpleStringSchema()))


    // 3.kafka-生产者 到 flink-消费 处理 flink-生产 到 kafka-消费者

    // 172.29.44.241运行bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "172.29.44.241:9092")
//    properties.setProperty("group.id", "consumer-group")
//    var stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//
//    stream = stream.map(_.toUpperCase)

    // 172.29.44.241运行/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest
//    stream.addSink(new FlinkKafkaProducer011[String]("172.29.44.241:9092", "sinktest", new SimpleStringSchema()))


    env.execute("sink")
  }
}
