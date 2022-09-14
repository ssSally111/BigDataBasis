package spark.text0405

import sparkutils.SparkUtils
import org.apache.spark.sql.functions._

object Demo02 {
  def main(args: Array[String]): Unit = {

    // TODO : 任务一 :子任务二

    val spark = SparkUtils().getSpark
    import spark.implicits._

    val df = SparkUtils().readHdfsCsv(spark, "/diliveryoutput1")


    // 1.针对数据集“客单价”属性，审查缺失值数量 customePrice - 218

    val missingNum = df.where($"customePrice" isNull).count()

    printf("===“客单价”属性缺失记录为%d条，缺失比例%.2f%%===\n", missingNum, missingNum / df.count().toDouble)


    // 2.当缺失值比例大于5%时，对缺失值字段进行均值填充

    // 平均值
    val avg = df
      .select($"customePrice")
      .where($"customePrice" isNotNull)
      .agg(mean($"customePrice"))
      .first()(0)
      .toString

    // 中位数
    val arr = df
      .select($"customePrice")
      .where($"customePrice" isNotNull)
      .sort($"customePrice" + 0)
      .collect()
    val middle = arr((arr.length / 2).toInt)(0).toString

    val resultDf1 = df.withColumn("customePrice", when(col("customePrice") isNull, avg).otherwise(col("customePrice")))
    val resultDf2 = df.na.fill(middle, Array("customePrice"))

    println(s"===“客单价”属性中位数为$middle===")

    SparkUtils().saveHDFS(resultDf2, "/diliveryoutput2")

  }
}
