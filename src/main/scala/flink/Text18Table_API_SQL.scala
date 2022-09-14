package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object Text18Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : kafka管道测试

    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)


    // 从kafka读取数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("pipeline-input")
      .property("zookeeper.connect", "172.29.44.241:2181")
      .property("bootstrap.servers", "172.29.44.241:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("field1", DataTypes.STRING())
        .field("field2", DataTypes.STRING())
        .field("field3", DataTypes.INT())
      )
      .createTemporaryTable("inputTable")

    val inputTable = tableEnv.from("inputTable")

    // 简单转换操作
    val resultTable = inputTable
      .select('field1, 'field3)
      .filter('field1.like("aa%"))


    // 聚合转换操作
    val aggTable = inputTable
      .groupBy('field1)
      .select('field1, 'field1.count as 'count)


    // 输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("pipeline-output")
      .property("zookeeper.connect", "172.29.44.241:2181")
      .property("bootstrap.servers", "172.29.44.241:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("field1", DataTypes.STRING())
        .field("field2", DataTypes.INT())
      )
      .createTemporaryTable("outputTable")

    resultTable.insertInto("outputTable")

    env.execute("kafka pipeline test")
  }
}
