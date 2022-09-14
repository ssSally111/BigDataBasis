package spark

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text06_sortBy {
  def main(args: Array[String]): Unit = {

    // TODO : sortBy

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 9, 3, 4, 4, 5, 8), 2)

    val value = rdd.sortBy(a => a)

    println(value.collect().mkString(","))

    //    =============================

    val rdd2 = spark.sparkContext.makeRDD(List(("1", 1), ("ayuky", 2), ("B2", 3)), 1)

    val value2 = rdd2.sortBy(b => b._1, false)

    println(value2.collect().mkString(","))

  }
}
