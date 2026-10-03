package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - mapPartitionsWithIndex
object Spark03_RDD_Transform_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(num => (index, num))
      }
    )

    mapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}