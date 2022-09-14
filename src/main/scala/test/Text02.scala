package date0321

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Text02 {
  def main(args: Array[String]): Unit = {
    //    将三个sql文件导入mysql,然后分别读入后合并为一个数据(meal_order_detail),将数据中的NA替换为空值.
    val spark = SparkSession.builder().master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._


    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val df = spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail1", prop).union(
      spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail2", prop)).union(
      spark.read.format("jdbc").jdbc("jdbc:mysql://localhost:3306/spark", "spark.meal_order_detail3", prop))
    //    ================ 以上读数据  ================

    df.rdd
      .filter(!_.isNullAt(0))
      .map(_.toSeq
        .map(value => {
          val newValue = value.toString.trim
          if (newValue.equals("NA")) "" else newValue
        })
      )
      .toDF()
      .show(false)

  }
}
