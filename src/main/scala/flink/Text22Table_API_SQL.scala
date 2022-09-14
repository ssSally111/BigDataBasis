package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, GroupWindow, Session, Slide, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Text22Table_API_SQL {
  def main(args: Array[String]): Unit = {

    // TODO : 分组窗口

    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val path = "src/main/resources/datas/text.txt"
    val data = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = data
      .map(_.split(","))
      .map(arr => SensorReading(arr(0), arr(1).toLong, arr(2).toDouble))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.date * 1L
      })

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'date.rowtime as 'ts, 'data)

    // table api
    val resultTable = sensorTable
      .window(Tumble over 2.seconds on 'ts as 'w)
      .groupBy('id, 'w)
      .select('id, 'id.count, 'data.avg, 'w.end)

    // sql
    tableEnv.createTemporaryView("sensor",sensorTable)
    val resultSql = tableEnv.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | avg(data),
        | tumble_end(ts, interval '10' second)
        |from sensor
        |group by
        | id,
        | tumble(ts, interval '10' second)
        |""".stripMargin
    )

    resultTable.toAppendStream[Row].print()
//    resultSql.toRetractStream[Row].print()

    /**
     * 窗口类型
     *

    // Tumbling Event-time Window
    val table1 = sensorTable
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Tumbling Processing - time Window
    val table2 = sensorTable
      .window(Tumble over 10.minutes on 'proctime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Tumbling Row - count Window
    val table3 = sensorTable
      .window(Tumble over 10.rows on 'proctime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // sliding Event - time Window
    val table4 = sensorTable
      .window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Sliding Processing - time Window
    val table5 = sensorTable
      .window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Sliding Row - count Window
    val table6 = sensorTable
      .window(Slide over 10.rows every 5.minutes on 'proctime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Session Event - time Window
    val table7 = sensorTable
      .window(Session withGap 10.rows on 'rowtime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

    // Session Processing - time Window
    val table8 = sensorTable
      .window(Session withGap 10.rows on 'proctime as 'w)
      .groupBy('w, 'id)
      .select('id, 'data.sum)

     */

//    sensorTable.toAppendStream[Row].print()

    env.execute("the processing time")
  }
}
