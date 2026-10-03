package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Key-Value 算子 mapValues：必须是 (K,V) 格式才能用
object Spark14_RDD_Transform_MapValues {
  def main(args: Array[String]): Unit = {

    // 1. 环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapValues")
    val sc = new SparkContext(conf)

    // ========================
    // 必须是 (Key, Value) 格式！
    // ========================
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))

    // 2. 使用 mapValues：只改 value，不改 key
    val result = rdd.mapValues(value => value * 10)

    // 3. 打印
    result.collect().foreach(println)

    sc.stop()
  }
}