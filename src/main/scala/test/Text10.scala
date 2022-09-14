import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Text10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo0317").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("file:///data.txt")
    println(lines.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).reduceByKey(_ + _).sortBy(_._2,ascending = false).first())

  }
}
