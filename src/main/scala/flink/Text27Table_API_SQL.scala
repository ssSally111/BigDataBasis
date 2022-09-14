package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object Text27Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : udf-表聚合函数

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
    val top2Temp = new Top2Temp()
    val resultTable = table
      .groupBy('id)
      .flatAggregate(top2Temp('data) as ('temp, 'rank))
      .select('id, 'temp, 'rank)

    resultTable.toRetractStream[Row].print("table api")


    env.execute("Table Agg functions")
  }


  // 提取所有温度值中最高的两个温度,输出(temp,rank)
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    // 实现计算聚合结果的函数accumulate
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 实现输出结果方法,最终处理完表调用
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect((acc.highestTemp,1))
      out.collect((acc.secondHighestTemp,2))
    }
  }

  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }


}
