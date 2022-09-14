package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

object Text17Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 查询转换

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

    // 使用table api
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'data)
      .filter('id === "sensor_1")
    resultTable.toAppendStream[(String, Double)].print()

    // sql
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, data
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin
    )
    resultSqlTable.toAppendStream[(String, Double)].print()


    env.execute("readFile")
  }
}
