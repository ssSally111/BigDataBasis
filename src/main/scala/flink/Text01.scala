package flink

import org.apache.flink.api.scala._

object Text01 {
  def main(args: Array[String]): Unit = {
    // TODO: 有界数据-批处理

    // 创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("src/main/resources/datas/data.txt")

    val res: DataSet[(String, Int)] = data
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    res.print()
  }
}
