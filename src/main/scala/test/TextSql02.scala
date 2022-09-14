object TextSql02{
        def main(args:Array[String]):Unit={
        val sc=new SparkContext(new SparkConf().setMaster("local[8]").setAppName("TextSql02"))
        val spark=SparkSession.builder().getOrCreate()


        val jdbcDF=spark.read.format("jdbc").
        option("url","jdbc:mysql://localhost:3306/spark").
        option("driver","com.mysql.jdbc.Driver").
        option("dbtable","test").
        option("user","root").
        option("password","root").
        load()
//
//    jdbcDF.printSchema()
//    jdbcDF.show(false)


        //    做临时表插入mysql数据库
        val schema=StructType(List(StructField("a",StringType,true),StructField("b",IntegerType,true)))
        val row=sc.parallelize(Array(("lisi",100),("wangwu",54))).map(p=>Row(p._1,p._2))
        val df=spark.createDataFrame(row,schema)
//
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    prop.put("driver", "com.mysql.jdbc.Driver")
//
//    df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark", "spark.test1", prop)
//

//
//    jdbcDF.show()

//    import spark.implicits._
//    Seq(("dongzi1", 485), ("dongzi2", 885), ("dongzi3", 55), ("dongzi4", 41), ("dongzi5", 135)).toDF("a","b").write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark", "spark.test1", prop)

        df.write.format("jdbc").
        option("url","jdbc:mysql://localhost:3306/spark").
        option("driver","com.mysql.jdbc.Driver").
        option("dbtable","test").
        option("user","root").
        option("password","root").
        save()
        }
        }
