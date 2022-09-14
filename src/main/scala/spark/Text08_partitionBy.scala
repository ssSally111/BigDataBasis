package spark

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text08_partitionBy {
  def main(args: Array[String]): Unit = {

    // TODO : partitionBy

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    val newRdd: RDD[(Int, Int)] = rdd
      .map((_, 1))
      .partitionBy(new HashPartitioner(2))

    newRdd.saveAsTextFile("output2")


    // 1.重分区器与当前RDD分区器相同怎么办?
    // Scala语言 中的双等号的作用其实进行了非空校验后进行equals操作 所以双等号不用于内存的比较
    newRdd.partitionBy(new HashPartitioner(2))

    // 2.spark还有其他分区器吗?

    // 3.按照自己的方法进行数据分析怎么办?

  }
}
