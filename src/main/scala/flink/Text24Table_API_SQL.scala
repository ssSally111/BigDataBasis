package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object Text24Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : udf-标量函数

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



    // 调用自定义hash函数,对id进行hash运算

    // table api
    val table = tableEnv.from("inputTable")
    // 首先new一个udf实例
    val hashCode = new HashCode(22)
    val resultTable = table
      .select('id, '_date, hashCode('id))

    // sql
    // 需要在环境中注册UDF
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id, _date, hashCode(id) from inputTable")


    resultTable.toAppendStream[Row].print("table api")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("Scalar functions")
  }


  class HashCode(factor: Int) extends ScalarFunction {
    def eval(str: String): Int = {
      str.hashCode * factor - 10000
    }
  }

}
