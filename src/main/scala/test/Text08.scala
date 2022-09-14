package date0321

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import sparkutils.SparkUtils
import scala.util.control.Breaks._

object Text08 {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local[8]").appName("Test").getOrCreate().sparkContext
    sc.setLogLevel("error")
    val spark = SparkUtils().getSpark
    import spark.implicits._

    // TODO； 模拟两个数据源, 进行连接查询(name, sex, height), 按照身高进行降序排序

    /*

        // (id, name, sex, birthday)
        val rdd1 = sc.parallelize(Array((1, "张三", "男", "20010212"), (2, "李四", "男", "20010621"), (3, "王五", "男", "20011108"), (4, "小明", "男", "20021224"), (5, "小红", "女", "20000821")))
        // (id, age, height)
        val rdd2 = sc.parallelize(Array((1, 25, 182), (2, 13, 185), (3, 34, 175), (4, 18, 178), (5, 29, 173)))

        // (id, (name, sex))
        val nameRdd = rdd1.map(
          tup => (tup._1, (tup._2, tup._3))
        )

        // (id, height)
        val heightRdd = rdd2.map(
          tup => (tup._1, tup._3)
        )

        nameRdd.join(heightRdd)
          .map(tup => (tup._2._1._1, (tup._2._1._2, tup._2._2))) // (name,(sex,height))
          .partitionBy(new HashPartitioner(1))
          .sortBy(_._2._2, ascending = false)
          .foreach(println)

     */


    val rdd = SparkUtils().readData(spark, "src/main/resources/datas/window", 1)

    val w = Window.orderBy("b").rowsBetween(0, 1)
    val c = last($"c", true).over(w)

    val result = rdd
      .map(row => {
        val arr = row.split(",")
        (arr(0), arr(1), arr(2))
      })
      .toDF("a", "b", "c")
    //      .withColumn("c",c)
    //      .show()

    val arr = result.collect()

    while (arr.distinct.length != arr.length) {
      for (i <- arr.indices) {
        if (arr(i).isNullAt(2)) {
          arr(i) = if (i == 0) arr(arr.length - 1) else arr(i - 1)
        }
      }
    }



  }
}
