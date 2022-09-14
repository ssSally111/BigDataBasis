object TextSql03{
        def main(args:Array[String]):Unit={
        val sc=new SparkContext(new SparkConf().setMaster("local[8]").setAppName("超级宇宙无敌数据库写入器"))
        val spark=SparkSession.builder().getOrCreate()

        val df:DataFrame=spark.read.csv("src/main/resources/datas/hotelorder.csv")
        val f=df.first()

import spark.implicits._
    df.filter(line=>line.size==9&&!line.equals(f)).map(line=>(line.get(0).toString,line.get(1).toString,line.get(2).toString,line.get(5).toString.toInt)).toDF("city","hotelName","checkinTime","price").write.format("jdbc").
            option("url","jdbc:mysql://localhost:3306/spark").
            option("driver","com.mysql.jdbc.Driver").
            option("dbtable","hotelorder").
            option("user","root").
            option("password","root").
            save()
            }
            }
