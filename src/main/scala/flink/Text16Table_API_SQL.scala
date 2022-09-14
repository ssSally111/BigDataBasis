package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object Text16Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 从kafka中读取数据

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("zookeeper.connect","172.29.44.241:2181")
        .property("bootstrap.servers","172.29.44.241:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("date", DataTypes.BIGINT())
        .field("data", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    tableEnv.from("inputTable").select('id,'data).toAppendStream[(String, Double)].print()


    env.execute("readKafka")
  }
}
