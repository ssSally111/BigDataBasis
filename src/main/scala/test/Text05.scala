package date0321

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Text05 {
  def main(args: Array[String]): Unit = {
    //    输出每列数据缺失值大于95%的列名,并将该列删除.
    val spark = SparkSession.builder().master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df4: DataFrame = spark.read.csv("src/main/resources/datas/users_info.csv")
    //    ================ 以上读数据  ================



    aaa(df4)

    def aaa(df: DataFrame): Unit = {
      var needColumnName: List[String] = List()
      val first = df.first()
      val newDf: Dataset[Row] = df.filter(!_.equals(first))
      newDf.createOrReplaceTempView("temptable")

      newDf.schema.map(_.name).foreach(columnName => {
        val missingCount = (newDf.count() - newDf.na.drop(Array(columnName)).count()) / newDf.count().toDouble
        if (missingCount < 0.95) {
          needColumnName = columnName :: needColumnName
        }
      })
      val temp = spark.sql(s"select ${needColumnName.mkString(",")} from temptable")
      temp.show(false)

//    在4的基础上,对users_info数据集的行进行空值数判断,输出有一个空值的行数,2个空值的行数……
      temp
        .map(line => {
          ((for (i <- 0 until line.length if line.isNullAt(i)) yield 1).sum,1)
        })
        .rdd
        .reduceByKey(_+_)
//        .filter(a=> Array(1,2).contains(a._1))
        .toDF("空值数","行数")
        .show()

    }
  }
}
