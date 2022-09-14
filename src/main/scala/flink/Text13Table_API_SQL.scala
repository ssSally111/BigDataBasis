package flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

object Text13Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 将DataStream转换成表

//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
//    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
//
//    val t1 = streamTableEnv.fromDataStream(_)
//    val t2 = tableEnv.fromDataSet(_)


  }
}
