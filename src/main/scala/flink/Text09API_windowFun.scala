package flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Text09API_windowFun {
  def main(args: Array[String]): Unit = {

    // TODO : 任务-每15秒统计一次,窗口内所有各个温度的最小值,以及最新的时间戳

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        (arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .reduce((curRes, newData) => {
        (curRes._1, newData._2, curRes._3.min(newData._3))
      })

    dataStream.print()

    env.execute()
  }
}
