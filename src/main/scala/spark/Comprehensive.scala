package spark

import java.util.Properties

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import sparkutils.SparkUtils

import scala.collection.mutable._

object Comprehensive {

  case class Hotelorde(city: String, hotelname: String, roomNum: Int, price: Int)


  def main(args: Array[String]): Unit = {

    // TODO : 小案例 - 统计 每一个市 每个酒店 每个房间个数价格 排行的 Top3

    /**
     * 数据源 hotelorder.csv
     * 字段 city,hotelname,checkinTime,roomNum,days,price,isOTA,OTArefuseOrder,directOrder
     */


    val spark = SparkUtils().getSpark
    import spark.implicits._

    val hotelorde: RDD[String] = SparkUtils().readData(spark, "src/main/resources/datas/hotelorder.csv", 8)
    val first = hotelorde.first()


    //    val result: RDD[(String, String, Int, Int)] = hotelorde
    //      .filter(!_.equals(first))
    //      .map(row => {
    //        val rowArr = row.split(",")
    //        (rowArr(0), rowArr(1), rowArr(3).toInt, rowArr(5).toInt)
    //      })
    //      .groupBy(rowTup => (rowTup._1, rowTup._2, rowTup._3))
    //      .mapValues(datas => {
    //
    //        val prices = ArrayBuffer[Int]()
    //        val iter = datas.iterator
    //        while (iter.hasNext) {
    //          prices += iter.next()._4
    //        }
    //        prices.sortBy(price => price).take(3)
    //      })
    //      .partitionBy(new HashPartitioner(1))
    //      .sortBy(row => {
    //        (row._1._1, row._1._2, row._1._3)
    //      })
    //      .flatMap(row => {
    //        row._2.map(price =>
    //          (row._1._1, row._1._2, row._1._3, price)
    //        )
    //      })
    //
    //
    //    result.foreach(println)

    //    val result2 = hotelorde
    //      .filter(!_.equals(first))
    //      .map(row => {
    //        val rowArr = row.split(",")
    //        (rowArr(0), rowArr(1), rowArr(3).toInt, rowArr(5).toInt)
    //      })
    //      .groupBy(rowTup => (rowTup._1, rowTup._2, rowTup._3))
    //      .flatMap(row => {
    //        val iter = row._2.iterator
    //        iter.toList.sortBy(rowTup => rowTup._4).take(3)
    //      })
    //      .sortBy(row => row)

    val result3 = hotelorde
      .filter(!_.equals(first))
      .map(row => {
        val rowArr = row.split(",")
        (rowArr(0), rowArr(1), rowArr(3).toInt, rowArr(5).toInt)
      })
      .groupBy(rowTup => (rowTup._1, rowTup._2, rowTup._3))
      .mapValues(datas => {

        var top1 = 0
        var top2 = 0
        var top3 = 0

        val iter = datas.iterator
        while (iter.hasNext) {
          val price = iter.next()._4
          if (price > top1) {
            top1 = price
          } else if (price > top2) {
            top2 = price
          } else if (price > top3) {
            top3 = price
          }
        }
        List(top1,top2,top3)
      })
      .partitionBy(new HashPartitioner(1))
      .sortBy(row => row._1)
      .flatMap(row => {
        row._2.map(price =>
          (row._1._1, row._1._2, row._1._3, price)
        )
      })

    result3.foreach(println)


    // 存入mysql

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")

    result3.toDF("city", "hotelname", "roomNum", "price")
      .write
      .mode("append")
      .jdbc("jdbc:mysql://172.29.44.241:3306/spark?useSSL=false", "hotelorde3", prop)

    Thread.sleep(1000000)
  }
}
