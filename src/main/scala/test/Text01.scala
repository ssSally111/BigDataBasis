package date0321

import org.apache.flink.api.java.tuple.Tuple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Text01 {
  def main(args: Array[String]): Unit = {
    //    读入两个 csv文件,计算每个数据列的最大最小值,均值,中位数,方差,数据数量并显示.
    val spark = SparkSession.builder()
      .master("local[8]")
      .appName("超级宇宙无敌异形数据终结者")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    val mealDF = spark.read.option("header", "true").option("encoding", "gbk").csv("src/main/resources/datas/meal_order_info.csv")
    val usersDF = spark.read.csv("src/main/resources/datas/users_info.csv")

    //    ================ 以上读数据  ================

    //    case class Person(name:String)
    //    val a: Seq[String] =mealDF.schema.map(_.name)
    //    val b: RDD[String] = spark.sparkContext.parallelize(a)
    //    b.map(Person).toDF()


    mealDF.schema
      .map(_.name)
      .map(colname => mealDF.agg(max(colname), min(colname), avg(colname), variance(colname), count(colname)))

//    val c:Seq[Tuple] = b.map(df => (df.first()(0), df.first()(1), df.first()(2), df.first()(3), df.first()(4)))

//    val cas = StructType(Array(StructField("max",StringType,true),StructField("min",StringType,true),StructField("avg",StringType,true),StructField("variance",StringType,true),StructField("count",StringType,true)))



    /*
    showMathData(mealDF)
    showMathData(usersDF)


    def showMathData(dataFrame: DataFrame): Unit = {
      (for (i <- 0 until firstRow.length if (if (towRow(i) == null) "" else towRow(i).toString).matches("\\d+")) yield (i, firstRow(i))).
        foreach(key => {
          dataFrame.select(dataFrame(s"_c${key._1}")).toDF(s"t").createOrReplaceTempView(s"table${key._1}")
          spark.sql(s"select max(t+0) as ${key._2}_max, min(t) as ${key._2}_min,avg(t) as ${key._2}_avg,percentile_approx(t,0.5) as zws,variance(t) as ${key._2}_variance, count(t) as ${key._2}_count from table${key._1}").show(false)
        })
    }
    */


  }
}
