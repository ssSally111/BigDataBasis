package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object Text08API_Timewindow {
  def main(args: Array[String]): Unit = {

    // TODO : TIME WINDOW

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")

//    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        (arr(0), arr(2).toDouble)
      })
      .keyBy(0)
//      .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(5)))
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//      .timeWindow(Time.seconds(15)) // 一个参数是滚动窗口简写,两个是滑动简写
      .countWindow(5) // 一个参数是滚动窗口简写,两个是滑动简写
      .reduce((a, b) => {
        (a._1, a._2.min(b._2))
      })

    dataStream.print()

    env.execute()
  }
}
