package date0321

import org.apache.flink.api.scala._

object Text07 {
  def main(args: Array[String]): Unit = {
    // 创建批处理执行环境
    val env =  ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("datas/data.txt")

    val res : DataSet[(String, Int)] = data
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    res.print()
  }
}
