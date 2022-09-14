package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text01_glom {
  def main(args: Array[String]): Unit = {

    // TODO : glom

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4))

    val value: RDD[Array[Int]] = rdd.glom()

    value.foreach(_.foreach(println))
  }
}
