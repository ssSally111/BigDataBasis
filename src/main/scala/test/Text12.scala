object Text12{
        def main(args:Array[String]):Unit={
        val sc=new SparkContext(new SparkConf().setAppName("Text03").setMaster("local[4]"))
        val rdd=sc.parallelize(Array(("spark",24),("spark",41),("spark",8),("spark",24),("hadoop",4),("hadoop",44),("hadoop",21),("hadoop",9)))

        rdd.mapValues((_,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x=>(x._1/x._2)).foreach(println)
        }
        }
