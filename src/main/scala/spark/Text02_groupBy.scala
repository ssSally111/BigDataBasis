package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text02_groupBy {
  def main(args: Array[String]): Unit = {

    // TODO : groupBy

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4))

    val result: RDD[(Int, Iterable[Int])] = rdd
      .groupBy(_ % 2)
      .map({
        case (key, iter) =>
          (key, iter.map(_ + 1))
      })

    result.foreach(println)

  }
}
