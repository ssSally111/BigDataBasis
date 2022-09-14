package date0321

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Text03 {
  def main(args: Array[String]): Unit = {
    //    对三个数据集显示每列的空值个数和缺失比例(百分比)
    val spark: SparkSession = SparkSession.builder().master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df1: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail1", prop)
    val df2: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail2", prop)
    val df3: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail3", prop)
    //    ================ 以上读数据  ================

    get(df1)
    println("===========================================")
    get(df2)
    println("===========================================")
    get(df3)


    def get(df: DataFrame): Unit = {
      df.schema.map(_.name).foreach(columnName => {
        //        println(df.select(columnName).where(s"${columnName} is null").count())
        val missingCount = df.select(columnName).filter(values => values.toString().trim.contains("NA") || values.toString().trim.equals("")).count()
        println((columnName, missingCount, s"${missingCount / df.select(columnName).count() * 100}%"))
      })
    }
  }
}
