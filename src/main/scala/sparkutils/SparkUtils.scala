package sparkutils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class SparkUtils() {
  def getSpark: SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("text")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    spark
  }

  def readData(spark: SparkSession, path: String, minPartitions: Int): RDD[String] = {
    spark.sparkContext.textFile(path, minPartitions)
  }

  def readHdfsCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").csv(s"hdfs://172.29.44.241:9000$path")
  }

  def saveHDFS(dataFrame: DataFrame, path: String): Unit = {
    dataFrame.coalesce(1).write.option("header", "true").csv(s"hdfs://172.29.44.241:9000$path")
  }
}

