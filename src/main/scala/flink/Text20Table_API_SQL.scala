package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object Text20Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 查看执行计划

    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "src/main/resources/datas/text.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("date", DataTypes.BIGINT())
        .field("data", DataTypes.DOUBLE())
//        .field("pt", DataTypes.TIMESTAMP(3)).proctime()
      )
      .createTemporaryTable("inputTable")

    val resultData = tableEnv.sqlQuery("select id, data from inputTable")

    val str: String = tableEnv.explain(resultData)
    println(str)

    env.execute("Viewing the Execution Plan")
  }
}
