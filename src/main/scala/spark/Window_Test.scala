package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sparkutils.SparkUtils

import scala.collection.immutable

object Window_Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils().getSpark
    import spark.implicits._


    val rdd: RDD[String] = SparkUtils().readData(spark,"src/main/resources/datas/window",4)

    val newRdd: RDD[(String, String, String)] = rdd.map(row => {val arr = row.split(",");(arr(0),arr(1),arr(2))})



    val df = newRdd.toDF("id", "key", "value")


    val w = Window.partitionBy('id).orderBy('key)

//    df.withColumn("id", lit(1)).withColumn('value,coalesce():_*)

    //    df.
  }
}
