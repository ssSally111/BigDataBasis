package spark

import sparkutils.SparkUtils

object Text10_combineByKey {
  def main(args: Array[String]): Unit = {

    // TODO : combineByKey

    val spark = SparkUtils().getSpark

    val rdd = spark.sparkContext.makeRDD(List(
      ("a", 1), ("b", 2), ("a", 3),
      ("b", 4), ("a", 5), ("b", 6)),
      2)

    rdd
      .combineByKey(
        (_, 1),
        (t: (Int, Int), v: Int) => (t._1 + v, t._2 + 1),
        (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
      )
      .mapValues(v => v._1 / v._2)
      .collect()
      .foreach(println)
  }
}
