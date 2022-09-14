package date0321

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object Text04 {
  def main(args: Array[String]): Unit = {
    //    对三个数据集,分别统计重复行的个数,并输出显示.
    val spark = SparkSession.builder().master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val df1: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail1", prop)
    val df2: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail2", prop)
    val df3: DataFrame = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail3", prop)
    //    ================ 以上读数据  ================

    println(("spark.meal_order_detail1", df1.count() - df1.distinct().count()))
    println(("spark.meal_order_detail2", df2.count() - df2.distinct().count()))
    println(("spark.meal_order_detail3", df3.count() - df3.distinct().count()))

    //    distinct：返回一个不包含重复记录的DataFrame
  }
}
