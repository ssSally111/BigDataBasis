package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object Text25Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : udf-表函数

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
    val split = new Split("_")
    val resultTable = table
      .joinLateral(split('id) as('word, 'length))
      .select('id, '_date, 'word, 'length)

    // sql
    // 需要在环境中注册UDF
    tableEnv.registerFunction("split", split)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | _date,
        | word,
        | length
        |from inputTable,
        | lateral table(split(id)) as splitid(word, length)
        |""".stripMargin)


    resultTable.toAppendStream[Row].print("table api")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("Table functions")
  }


  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(
        word => collect((word, word.length))
      )
    }
  }

}
