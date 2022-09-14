package flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Text07API_Sink_mysql {
  def main(args: Array[String]): Unit = {
    // TODO: API_Sink_mysql

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //     读数据
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    dataStream.addSink(new MyJdbcSinkFunc)


    env.execute("mysql-test1")
  }
}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
  // 定义连接, 预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://172.29.44.241:3306/flink_test?useSSL=false", "root", "root")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 先执行更新操作, 查到就更新
    updateStmt.setDouble(1, value.data)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 如果更新没有查到数据, 那么就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.data)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    updateStmt.close()
    insertStmt.close()
    conn.close()
  }
}
