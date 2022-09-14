object Text11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Text02")
    val sc = new SparkContext(conf)

    val list = List(("spark", 12.0), ("spark", 27.0), ("hadoop", 32.0), ("hadoop", 82.0))
    sc.parallelize(list).combineByKey(
      (1, _),
      (c: (Int, Double), newValue) => (c._1 + 1, c._2 + newValue),
      (c1: (Int, Double), c2: (Int, Double)) => (c1._1 + c2._1, c1._2 + c2._2)
    ).mapValues(x => x._2 / x._1).foreach(println)







    println("=================================")









    sc.parallelize(list).combineByKey(
      a => {  // 传进value进行操作初始化
        println(s"a : ${a}")
        (1, a)
      },
      (c: (Int, Double), newValue) => { // 在每个分区内执行
        println(s"oldVal : (${c._1} , ${c._2}) --- newVal : ${newValue}")
        (c._1 + 1, c._2 + newValue)
      },
      (c1: (Int, Double), c2: (Int, Double)) => { // 在不同分区间进行
        println(s"c1 : (${c1._1} , ${c1._2}) --- c2 : (${c2._1} , ${c2._2})")
        (c1._1 + c2._1, c1._2 + c2._2)
      }
    ).count()


    Thread.sleep(1000000)
  }

}
