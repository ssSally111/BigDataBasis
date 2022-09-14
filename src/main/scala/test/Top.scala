object Top{
        def main(args:Array[String]):Unit={
        val sc=new SparkContext(new SparkConf().setMaster("local[8]").setAppName("Top"))
        sc.setLogLevel("ERROR")

        //     模拟任务
        //        val rdd = sc.textFile("file:///home/icetea/Desktop/hotelorder.csv",8)
        //        rdd.saveAsTextFile("file:///home/icetea/Desktop/datas")


        val datas=sc.textFile("file:///home/icetea/Desktop/datas")
        datas.persist(StorageLevel.MEMORY_ONLY)
        val first=datas.first()


        // 任务求TOP(3)
        //     datas.filter(line => line.trim.split(",").length == 9 && !line.equals(first)).sortBy(_.split(",")(5), ascending = false).take(3).foreach(println)


        // 最大最小
        //    val newData = datas.filter(line => line.trim.split(",").length == 9 && !line.equals(first)).sortBy(_.split(",")(5)).collect()
        //    println(s"最小 : ${newData(0)}\n最大 : ${newData(newData.length - 1)}")
        //
        //    Thread.sleep(1000000)       (1, "芜湖市,浪漫满屋酒店,2021-01-07 00:00:00,3,2,570,Y,Y,N")
        type Hotel=(Int,Int)

        datas.filter(line=>line.trim.split(",").length==9&&!line.equals(first)).map(line=>(line.split(",")(0),line.split(",")(5).toInt)).combineByKey(
        value=>{
        (1,value)
        },
        (oldValue:Hotel,newValue)=>{
        (oldValue._1+1,oldValue._2+newValue)
        },
        (a1:Hotel,a2:Hotel)=>{
        (a1._1+a2._1,a1._2+a2._2)
        }
        ).mapValues(a=>a._2/a._1).foreach(println)

        Thread.sleep(1000000)
        }
        }
