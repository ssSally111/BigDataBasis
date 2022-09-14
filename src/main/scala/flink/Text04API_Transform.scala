package flink

import org.apache.flink.streaming.api.scala._

object Text04API_Transform {
  def main(args: Array[String]): Unit = {
    // TODO: 转换操作

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读数据
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")

    // 一 简单转换操作
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })


    // 二 聚合
    // 1.操作：求最小温度data的信息(分组聚合1)
    val aggStream1 = dataStream
      .keyBy(_.id)
      .minBy("data")

//        aggStream1.print()


    // 2.操作：求最小温度data,最近时间date的信息要用Reduce(分组聚合2)
    val aggStream2 = dataStream
      .keyBy(_.id)
      .reduce((curState, newData) => {
        SensorReading(curState.id, newData.date, curState.data.min(newData.data))
      })

//        aggStream2.print()


    // 三 多流转换操作

    // 1.需求：按照传感器温度高低（以30为界）拆分成两个流 (分流操作)
    val splitStream = dataStream
      .split(data => {
        if (data.data > 30) Seq("high") else Seq("low")
      })
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

//        highTempStream.print("high")
//        lowTempStream.print("low")
    //    allTempStream.print("all")


    // 2-1. 合流. connect
    val warningStream = highTempStream.map(data => (data.id, data.data))
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapResultStream = connectedStreams
      .map(
        warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )

//    coMapResultStream.print("coMap")

    // 2-2. 合流. union
    val unionStream = highTempStream.union(lowTempStream,allTempStream)

//    unionStream.print("union")

    env.execute("超级宇宙无敌异形转换操作实验终结者")
  }
}
