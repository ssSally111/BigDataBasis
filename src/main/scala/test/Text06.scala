package date0321

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object Text06 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[8]")
      .enableHiveSupport()  // spark 对 hive 的支持
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df: DataFrame = spark.read.option("header", "true").csv("src/main/resources/datas/users_info.csv")

    //    ================ 以上读数据  ================


    //    对users_info中的缺失值,按照如下规则进行填充.


    //    (1) FIRST_VISIT字段使用前项填充(使用缺失值之前一行的数据填充),如果第一行为空,就使用从最后一行填充.输出填充的数据个数
    val visitDf = df.select("FIRST_VISIT")
    var n = 0
    val rowArr = visitDf.collect()
    for (_ <- 1 to 2) {
      for (i <- rowArr.indices) {
        if (rowArr(i).isNullAt(0)) {
          rowArr(i) = if (i == 0) rowArr(rowArr.length - 1) else rowArr(i - 1)
          n += 1
        }
      }
    }

    println(n)
    //    rowArr.foreach(println)


    //    (2) LAST_VISITS字段缺失值使用如下方式填充: 当前行FIRST_VISIT字段+(LAST_VISITS-FIRST_VISIT).mean()(LAST_VISITS-FIRST_VISIT).mean()表示所有不为空的数据的两个字段之差的均值.填充完成后,输出填充的数据个数.
    val newDf = df.select("LAST_VISITS", "FIRST_VISIT").na.drop()


    //      .map(row => {
    //      row(0).toString.filter(_.toString.matches("\\d")).toLong - row(1).toString.filter(_.toString.matches("\\d")).toLong
    //
    //    })
    //

    //        newDf.withColumn("aaa", col("LAST_VISITS").cast("datetime")).show()
    //    spark.sql("select convert(datetime, LAST_VISITS)").show()


    //    newDf.withColumn("NEW_VISITS", datediff(newDf("LAST_VISITS").cast("date"), newDf("FIRST_VISIT").cast("date"))).show()
    //
    //    newDf.withColumn("newdate", unix_timestamp(newDf("LAST_VISITS")) - unix_timestamp(newDf("FIRST_VISIT"))).show()

    val name = Seq("aaa")

    val _avg = newDf.map(row => {
      val simpleDate = new SimpleDateFormat("yyyy/MM/dd hh:mm")
      simpleDate.parse(row(0).toString).getTime - simpleDate.parse(row(1).toString).getTime
    }).toDF(name: _*).select(avg("aaa").as("avg")).first()(0).toString.toDouble.toInt


    var n2 = 0
    newDf.map(row => {
      if (row(0) != null && row(1) != null) {
        n2 += 1
        println(n2)
        val simpleDate = new SimpleDateFormat("yyyy/MM/dd hh:mm")
        simpleDate.format(simpleDate.parse(row(1).toString).getTime + _avg)
      } else {
        null
      }
    }).show()


    val dfp = Seq(("2015/05/12 09:19", "2016/06/18 09:19")).toDF("aaa", "bbb")
    dfp.show()
    val date1 =  to_date(date_format(to_utc_timestamp(unix_timestamp(dfp("aaa"), "yyyy/MM/dd hh:mm"),"yyyy-MM-dd hh:mm"),"yyyy-MM-dd hh:mm"))
    val date2 =  to_date(date_format(to_utc_timestamp(unix_timestamp(dfp("bbb"), "yyyy/MM/dd hh:mm"),"yyyy-MM-dd hh:mm"),"yyyy-MM-dd hh:mm"))

    dfp.withColumn("dasd", datediff(date1, date2)).show()

  }
}
