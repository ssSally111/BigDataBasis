package spark

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text03_sample {
  def main(args: Array[String]): Unit = {

    // TODO : sample

    val spark = SparkUtils().getSpark

    val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))


    /**
     * 根据指定的规则从数据集中抽取数据
     *
     * 参1 bool:抽取完是否放回去,true:泊松算法,false:伯努利算法
     * 参2 double:如果抽取不放回:每个数据抽取到概率(基准值的概念),
     *           如果抽取放回:数据源中每条数据抽取的可能次数,
     * 参3 long:随机数种子,如果不传则使用当前系统时间)
     */
    println(rdd.sample(
      false,
      0.4,
      1
    ).collect().mkString(","))


    println(rdd.sample(
      false,
      0.5
    ).collect().mkString(","))

    println(rdd.sample(
      true,
      2
    ).collect().mkString(","))


  }
}
