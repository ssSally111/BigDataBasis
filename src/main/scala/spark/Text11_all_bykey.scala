package spark

import sparkutils.SparkUtils

object Text11_all_bykey {
  def main(args: Array[String]): Unit = {

// TODO : 四种函数区别

val spark = SparkUtils().getSpark

val rdd = spark.sparkContext.makeRDD(List(
  ("a", 1), ("b", 2), ("a", 3),
  ("b", 4), ("a", 5), ("b", 6)),
  2)


rdd.reduceByKey(_ + _) // wordcount
rdd.aggregateByKey(0)(_ + _, _ + _) // wordcount
rdd.foldByKey(0)(_ + _) // wordcount
rdd.combineByKey(t=>t, (t: Int, v: Int) => t + v, (t1: Int, t2: Int) => t1 + t2)

/*


reduceByKey:

combineByKeyWithClassTag[V](
(v: V) =>
 v,   // 第一个值不会参与计算
 func,  // 分区内 - 计算规则相同
 func,  // 分区间
 partitioner)


aggregateByKey:

combineByKeyWithClassTag[U](
(v: V) =>
 cleanedSeqOp(createZero(), v),   // 初始值和第一个key的value进行分区内的操作
 cleanedSeqOp,  // 分区内
 combOp,  // 分区间
 partitioner)


foldByKey:

combineByKeyWithClassTag[V](
(v: V) =>
 cleanedFunc(createZero(), v),  // 初始值和第一个key的value进行分区内的操作
 cleanedFunc, // 分区内  - 计算规则相同
 cleanedFunc, // 分区间
 partitioner)


combineByKey:

combineByKeyWithClassTag(
 createCombiner,    //相同key第一条数据进行处理函数
 mergeValue,  // 分区内
 mergeCombiners,  // 分区间
 defaultPartitioner(self))

 */
  }
}
