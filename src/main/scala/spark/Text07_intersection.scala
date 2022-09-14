package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text07_intersection {
  def main(args: Array[String]): Unit = {

    // TODO : 双Value类型

    val spark = SparkUtils().getSpark

    val rdd1: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 7))
    val rdd2: RDD[Int] = spark.sparkContext.makeRDD(List(5, 2, 3, 4))


    // 交集
    val value1 = rdd1.intersection(rdd2)


    // 并集
    val value2 = rdd1.union(rdd2)


    // 差集
    val value3 = rdd1.subtract(rdd2)

    // 拉链
    val value4 = rdd1.zip(rdd2)

    val rdd3= spark.sparkContext.makeRDD(List("g", "g2", "h3he", "vds4"))
    val value5 = rdd1.zip(rdd3)

//    Can only zip RDDs with same number of elements in each partition
    val rdd4 = spark.sparkContext.makeRDD(List("g", "g2", "h3he", "vds4","g"))
//    val value6 = rdd3.zip(rdd4)

//    Can't zip RDDs with unequal numbers of partitions: List(4, 2)
    val rdd5 = spark.sparkContext.makeRDD(List("g", "g2", "h3he", "vds4"),4)
    val rdd6= spark.sparkContext.makeRDD(List("g", "g2", "h3he", "vds4"),2)
//    val value7 = rdd5.zip(rdd6)

    println(value1.collect().mkString(","))
    println(value2.collect().mkString(","))
    println(value3.collect().mkString(","))
    println(value4.collect().mkString(","))

    println(value5.collect().mkString(","))
//    println(value6.collect().mkString(","))
//    println(value7.collect().mkString(","))
  }
}
