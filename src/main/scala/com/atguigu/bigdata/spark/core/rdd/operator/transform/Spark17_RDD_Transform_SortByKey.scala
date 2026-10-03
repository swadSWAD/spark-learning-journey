package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Key-Value 算子：sortByKey
object Spark17_RDD_Transform_SortByKey {
  def main(args: Array[String]): Unit = {

    // 1. 环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")
    val sc = new SparkContext(conf)

    // 2. Key-Value 数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("c", 3),
      ("a", 1),
      ("b", 2),
      ("a", 5)
    ))

    // 3. sortByKey：按 key 排序（默认升序）
    val result = rdd.sortByKey()

    // 4. 打印
    result.collect().foreach(println)

    sc.stop()
  }
}