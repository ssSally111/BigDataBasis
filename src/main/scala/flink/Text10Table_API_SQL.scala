package flink

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Text10Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : Table API 示例

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //     读数据
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })


    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 调用table api 进行转换
    val resultTable = dataTable
      .select("id, data")
      .filter("id == 'sensor_1'")

    // 直接用sql实现,需要注册
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql: String = "select id, data from dataTable where id = 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)


    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("result sql")

    env.execute("table-api-test")
  }
}
