package flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._

object Text12Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : Table API 表执行环境


    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)


    // 1.1 基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于build planner的流处理
    val blinkStreamSettings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings)

    // 1.4 基于build planner的批处理
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
//    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)


    env.execute("table-api-test1")
  }
}
