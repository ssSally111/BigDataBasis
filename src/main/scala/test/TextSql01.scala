object TextSql01{
        def main(args:Array[String]):Unit={
        val sc=new SparkContext(new SparkConf().setMaster("local[8]").setAppName("TextSql01"))
        val spark=SparkSession.builder().getOrCreate()

        // 构建表头
        val fields=Array(StructField("city",StringType,true),StructField("hotelname",StringType,true),StructField("price",IntegerType,true))
        val schema=StructType(fields)

        val dataRdd:RDD[String]=sc.textFile("file:///home/icetea/Desktop/hotelorder.csv")
        val first=dataRdd.first()

        // 构建内容
        val row=dataRdd.filter(line=>line.split(",").length==9&&!line.equals(first)).map(_.split(",")).map(arr=>Row(arr(0),arr(1),arr(5).trim.toInt))

        val length=row.count().toInt

        // 创建DataFrame
        val df=spark.createDataFrame(row,schema)

        df.createOrReplaceTempView("hotel")

        val results=spark.sql("select hotelname as name,price from hotel where price>890 and price<900 and hotelname like '%帝%' order by price desc")
        results.show(results.count().toInt,false)

        println("================================================================")

import spark.implicits._
    results.map(n=>s"name : ${n(0)} --- price : ${n(1)}").show(false)

            }
            }
