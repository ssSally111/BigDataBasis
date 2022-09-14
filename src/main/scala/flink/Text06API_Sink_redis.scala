package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Text06API_Sink_redis {
  def main(args: Array[String]): Unit = {
    // TODO: API_Sink_redis

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //     读数据
    val inputStream = env.readTextFile("src/main/resources/datas/text.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("172.29.44.241")
      .setPort(6379)
      .setPassword("root")
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    env.execute("redis-test")
  }
}

class MyRedisMapper extends RedisMapper[SensorReading] {

  // 定义保存数据到redis的命令 HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

  // 将ID设置为key
  override def getKeyFromData(t: SensorReading): String = t.id

  // 将温度值设置成value
  override def getValueFromData(t: SensorReading): String = t.data.toString
}