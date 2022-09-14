package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text09_aggregateByKey {
  def main(args: Array[String]): Unit = {

    // TODO : aggregateByKey

    val spark = SparkUtils().getSpark

    val rdd = spark.sparkContext.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    val rdd2 = spark.sparkContext.makeRDD(List(
      ("a", 1), ("b", 2), ("a", 3),
      ("b", 4), ("a", 5), ("b", 6)),
      2)

    val value = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    value.collect().foreach(println)


    rdd2.aggregateByKey(0)(math.max, _ + _).foreach(println)

    rdd2.foldByKey(0)(_ + _).collect().foreach(println)

    // 相同key的平均数
    rdd2
      .aggregateByKey((0, 0))(
        (t, v) => {
          (t._1 + v, t._2 + 1)
        },
        (t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2)
        }
      )
      .mapValues({ case (num, counst) => num / counst })
      .collect()
      .foreach(println)
  }
}
