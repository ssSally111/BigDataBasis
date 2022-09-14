package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object Text15Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 从文件中读数据

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

    tableEnv.sqlQuery("select * from inputTable").toAppendStream[(String, Long, Double)].print()

    env.execute("readFile")
  }
}
