package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object Text26Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : udf-聚合函数

    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "src/main/resources/datas/text.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("_date", DataTypes.BIGINT())
        .field("data", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")


    // table api
    val table = tableEnv.from("inputTable")
    // 首先new一个udf实例
    val avgTemp = new AvgTemp()
    val resultTable = table
      .groupBy('id)
      .aggregate(avgTemp('data) as 'avgTemp)
      .select('id, 'avgTemp)

    // sql
    // 需要在环境中注册UDF
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | avgTemp(data)
        |from inputTable
        |group by id
        |""".stripMargin)


    resultTable.toRetractStream[Row].print("table api")
    resultSqlTable.toRetractStream[Row].print("sql")

    env.execute("Table functions")
  }


  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {

    override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }

  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

}
