package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

object Text19Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 输出到mysql

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
      )
      .createTemporaryTable("inputTable")

    val resultData = tableEnv.sqlQuery("select id, data from inputTable")

    val sinkDDL: String =
      """
        |create table outputTest (
        |id varchar(20) not null,
        |data double not null
        |) with (
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://172.29.44.241:3306/test?useSSL=false',
        |'connector.table' = 'sensor_count',
        |'connector.driver' = 'com.mysql.jdbc.Driver',
        |'connector.username' = 'root',
        |'connector.password' = 'root'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL创建表
    resultData.insertInto("outputTest")

    env.execute("mysql test")
  }
}
