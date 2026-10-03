package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - flatMap
object Spark04_RDD_Transform_FlatMap {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    val rdd = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6)))

    val flatRDD: RDD[Int] = rdd.flatMap(
      list => list
    )

    flatRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
