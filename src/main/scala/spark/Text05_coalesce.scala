package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text05_coalesce {
  def main(args: Array[String]): Unit = {

    // TODO : coalesce

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // 默认情况下不会把分区内数据打乱重新组合 - 数据倾斜
    //    rdd.coalesce(2).saveAsTextFile("output")


//    rdd.coalesce(2, true).saveAsTextFile("output")



//    rdd.coalesce(3, true).saveAsTextFile("output")


//    简化操作
    rdd.repartition(3).saveAsTextFile("output")

  }
}
