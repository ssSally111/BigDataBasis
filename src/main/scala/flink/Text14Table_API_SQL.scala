package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object Text14Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 输出表

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "src/main/resources/datas/text.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("date", DataTypes.BIGINT())
        .field("data", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val sensorTable = tableEnv.from("inputTable")

    // 1.简单转换操作
    val resultTable = sensorTable
      .select('id, 'data)
      .filter('id === "sensor_1")

    // 2.聚合转换操作
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

        resultTable.toAppendStream[(String, Double)].print("result")
        aggTable.toRetractStream[(String, Long)].print("agg")

    // 3.注册输出表
    val outPutPath = "src/main/resources/datas/outText.txt"

    tableEnv.connect(new FileSystem().path(outPutPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
        //          .field("cnt",DataTypes.BIGINT())
      )
      .createTemporaryTable("outPutTable")

    resultTable.insertInto("outPutTable")


    env.execute("table")
  }
}
