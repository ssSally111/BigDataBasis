package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text04_distinct {
  def main(args: Array[String]): Unit = {

    // TODO : distinct

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4, 1, 2, 3))

    rdd.map((_, null))
      .reduceByKey((x, y) =>
        x, 1)


    // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
    rdd.distinct().collect().foreach(println)


    List(1, 2, 3, 4, 1, 2, 3).distinct
  }
}
