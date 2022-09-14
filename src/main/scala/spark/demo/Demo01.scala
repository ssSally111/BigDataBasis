package spark.text0405

import sparkutils.SparkUtils

object Demo01 {
  def main(args: Array[String]): Unit = {

    // TODO : 任务一 :子任务一 id,province, city, name, score, comment_number, good_comment, month_sales, avg_price, customePrice, category, delivery_time, min_price,shipping_fee, lat, lng, hours, pushNum, receiveNum, position, platform, tagType, battleGroup, complainNum

    val spark = SparkUtils().getSpark
    import spark.implicits._

    val path = "/platform_data/waimai01.csv"
    val df = SparkUtils().readHdfsCsv(spark, path)


    // 1.剔除属性列“推单数”小于“接单数”的异常数据条目 - 74


    val newDf = df.where($" pushNum" + 0 < $" receiveNum" + 0)

    val resultDf = df
      .except(newDf)
      .where($"id" !== "['id', 'province', 'city', 'name', 'score', 'comment_number', 'good_comment', 'month_sales', 'avg_price', 'customePrice', 'category', 'delivery_time', 'min_price', 'shipping_fee', 'lat', 'lng', 'hours', 'pushNum', 'receiveNum', 'position', 'platform', 'tagType', 'battleGroup', 'complainNum']")


    println(s"===“推单数”小于“接单数”的异常数据条数为${newDf.count()}条===")

    // 向hdfs写入结果集
    SparkUtils().saveHDFS(resultDf,"/diliveryoutput1")

  }
}
