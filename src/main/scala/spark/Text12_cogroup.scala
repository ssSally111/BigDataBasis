package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text12_cogroup {
  def main(args: Array[String]): Unit = {

    // TODO : cogroup

    val spark = SparkUtils().getSpark

    val rdd = spark.sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)),
      2)
    val rdd2 = spark.sparkContext.makeRDD(List(
      ("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5), ("b", 6)),
      2)

    val newRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd2)

    newRdd.foreach(println)





  }
}
