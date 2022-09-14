package text01

import org.apache.spark.rdd.RDD
import sparkutils.SparkUtils

object Text09 {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils().getSpark
    var mealData = SparkUtils().readData(spark, "src/main/resources/datas/meal_order_info.csv",8)
    val first = mealData.first()
    mealData = mealData.filter(!_.equals(first))

    // TODO: 离线数据统计

    // 1. 统计 菜数 数量: (id, 菜数数量)  dishes_count

    val dishes_count = mealData.map(
      row => {
        val rowArr = row.split(",")
        (rowArr(0), rowArr(7))
      }
    )


    // 2. 统计 支出 数量: (id, 支出数量)  expenditure

    val expenditure = mealData.map(
      row => {
        val rowArr = row.split(",")
        (rowArr(0), rowArr(6))
      }
    )


    // 3. 统计 应付账款 数量: (id, 应付账款数量)  accounts_payable

    val accounts_payable = mealData.map(
      row => {
        val rowArr = row.split(",")
        (rowArr(0), rowArr(8))
      }
    )


    // 4. 排序,并且取前十 (菜数排序,支出排序,应付账款排序)

    val cogroupRDD: RDD[(String, (Iterable[String], Iterable[String], Iterable[String]))] =
      dishes_count.cogroup(expenditure, accounts_payable)

    val analysisRDD = cogroupRDD.mapValues({
      case (dishes_countIter, expenditureIter, accounts_payableIter) => {

        var dishes_countCot = 0
        val iter1 = dishes_countIter.iterator
        if (iter1.hasNext) {
          dishes_countCot = iter1.next().toInt
        }
        var expenditureCot = 0
        val iter2 = expenditureIter.iterator
        if (iter2.hasNext) {
          expenditureCot = iter2.next().toInt
        }
        var accounts_payableCot = 0
        val iter3 = accounts_payableIter.iterator
        if (iter3.hasNext) {
          accounts_payableCot = iter3.next().toInt
        }

        (dishes_countCot, expenditureCot, accounts_payableCot)
      }
    })

    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)


    // 5. 打印输出
    resultRDD.foreach(println)

    spark.stop()
  }
}
