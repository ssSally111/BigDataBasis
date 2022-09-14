package flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object Text07API_Sink_mysql2 {
  def main(args: Array[String]): Unit = {
    // TODO: API_Sink_mysql2 实时更新-监控

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 产生数据
    val stream = env.addSource(new MySensorSource2)
    // 更新mysql数据库
    stream.addSink(new MyJdbcSinkFunc2)

    env.execute("mysql-test2")
  }
}

// mysql
class MyJdbcSinkFunc2() extends RichSinkFunction[SensorReading] {
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

// 自定义SourceFunction
class MySensorSource2() extends SourceFunction[SensorReading] {
  // 定义一个标识符flag,表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 随机生成数据(id,data)
    val rand = new scala.util.Random()
    var curTemp = 1.to(10).map(i => (s"sensor_${i}", rand.nextDouble() * 100))

    // 不停产生产数据，除非被cancel
    while (running) {
      // 微调数据
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取时间戳
      val curTime = System.currentTimeMillis()

      // 加入当前时间戳,调用sourceContext.collect发出数据
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔500ms
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = running = false
}