package flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object Text02 {
  def main(args: Array[String]): Unit = {
    // TODO: 无界数据-流处理

    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(32)

    // 从外部命令提取参数作为socket主机名和端口号
    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("hostname")
    val port = paramTool.getInt("port")

    // 接受一个socket文本流
    val inputDataStream = env.socketTextStream(host, port)

    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    // 启动任务执行
    env.execute("超级宇宙无敌异形流处理终结者")
  }
}
