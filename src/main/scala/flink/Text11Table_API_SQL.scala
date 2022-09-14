package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Text11Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : Table API 基本程序结构

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // 创建表的执行环境
    val tableEnv:StreamTableEnvironment = ???

    // 创建一张表用于读取数据-连接外部数据读取
    tableEnv.connect(???).createTemporaryTable("inputTable")

    // 注册一张表,用于把计算结果输出-连接外部写入数据
    tableEnv.connect(???).createTemporaryTable("outTable")

    // 通过Table API 查询算子,得到一张结果表
    val result: Table = tableEnv.from("inputTable").select(???)

    // 通过 SQL 查询语句,得到一张结果表
    val sqlResult = tableEnv.sqlQuery("select ... from inputTable ...")

    // 将结果表写入输出表中
    result.insertInto("outputTable")


    env.execute("table-api-test")
  }
}
