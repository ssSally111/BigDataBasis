package spark.text0405

import org.apache.spark.sql.{Column, DataFrame}
import sparkutils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Demo03 {
  def main(args: Array[String]): Unit = {

    // TODO : 任务二 数据挖掘  comment number, good comment / 评论数，好评论

    val spark = SparkUtils().getSpark
    import spark.implicits._

    val df = SparkUtils().readHdfsCsv(spark, "/diliveryoutput2")

    // 1.计算商家好评比，将“好评比”作为新属性添加至属性“评价数”后 【好评比计算公式：好评比=好评数/评价数】

    val resultDf1 = df
      .withColumn("commentProportion", concat(bround((df("good_comment") / df("comment_number") * 100).cast(DoubleType), 2), lit("%")))
    SparkUtils().saveHDFS(resultDf1,"/diliveryoutput3")


    // 2.筛选4项核心属性集：“商户业务包”，“接单数”，“客单价”，“好评比”，数据记录以接单数降序排列  tagType, receiveNum, customePrice, commentproportion

    val resultDf2 = resultDf1
      .select("tagType", "receiveNum", "customePrice", "commentProportion")
      .orderBy((col("receiveNum") + 0).desc_nulls_last)
    SparkUtils().saveHDFS(resultDf2,"/diliveryoutput4")


    // 3.对属性“接单数”，“客单价”进行max-min归一化，以实现对核心属性的预处理。将处理后的结果数据集以接单数降序排列 【归一化公式：x' = (x - X_min) / (X_max - X_min)】

    val noRepeatMap = resultDf2
      .select("tagType")
      .distinct()
      .collect()
      .zipWithIndex
      .map(v => v._1(0).toString -> v._2.toString)
      .toMap

    val resultDf3 = resultDf2
      .na.replace("tagType", noRepeatMap)
      .withColumn("receiveNum", getMax_Min(resultDf2, $"receiveNum", 2))
      .withColumn("customePrice", getMax_Min(resultDf2, $"customePrice", 2))
      .orderBy($"receiveNum".desc)

    SparkUtils().saveHDFS(resultDf3,"/diliveryoutput5")
  }

  def getMax_Min(dataFrame: DataFrame, column: Column, scale: Int): Column = {
    val receiveNum_max = dataFrame.select(max(column + 0)).first()(0).toString.toDouble
    val receiveNum_min = dataFrame.select(min(column + 0)).first()(0).toString.toDouble
    bround((column - receiveNum_min) / (receiveNum_max - receiveNum_min), scale)
  }
}
