package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkOperatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("OperatorTest")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 1. map：一对一转换
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val mapRDD = rdd1.map(_ * 10)
    println("map结果：")
    mapRDD.foreach(println)

    // 2. flatMap：一对多展开
    val rdd2 = sc.parallelize(List("hello spark", "hello world", "spark hive"))
    val flatMapRDD = rdd2.flatMap(_.split(" "))
    println("flatMap结果：")
    flatMapRDD.foreach(println)

    // 3. filter：过滤
    val rdd3 = sc.parallelize(List(1,2,3,4,5,6))
    val filterRDD = rdd3.filter(_ % 2 == 0)
    println("filter偶数：")
    filterRDD.foreach(println)

    // 4. reduceByKey：按key聚合
    val rdd4 = sc.parallelize(List(("a",1),("a",2),("b",1),("b",3)))
    val reduceRDD = rdd4.reduceByKey(_ + _)
    println("reduceByKey结果：")
    reduceRDD.foreach(println)

    // 5. sortBy：排序
    val rdd5 = sc.parallelize(List(3,1,4,1,5,9))
    val sortRDD = rdd5.sortBy(x => x, ascending = false)
    println("降序排序：")
    sortRDD.foreach(println)

    sc.stop()
  }
}