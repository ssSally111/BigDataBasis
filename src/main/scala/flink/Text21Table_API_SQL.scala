package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Text21Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 时间特性 1-定义处理时间

    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    val path = "src/main/resources/datas/text.txt"
    val data = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = data
      .map(_.split(","))
      .map(arr => SensorReading(arr(0), arr(1).toLong, arr(2).toDouble))

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'date as 'temperature, 'data, 'pt.proctime)
    sensorTable.printSchema()
    sensorTable.toAppendStream[Row].print()

    env.execute("the processing time")
  }
}
