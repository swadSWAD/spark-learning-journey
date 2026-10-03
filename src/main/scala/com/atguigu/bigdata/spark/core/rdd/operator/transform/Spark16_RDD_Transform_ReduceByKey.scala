package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Key-Value 算子：reduceByKey
object Spark16_RDD_Transform_ReduceByKey {
  def main(args: Array[String]): Unit = {

    // 1. 环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey")
    val sc = new SparkContext(conf)

    // 2. Key-Value 数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("a", 2),
      ("b", 3),
      ("b", 4)
    ))

    // 3. reduceByKey：相同key的value相加
    val result: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    // 4. 打印
    result.collect().foreach(println)

    sc.stop()
  }
}