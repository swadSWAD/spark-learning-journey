package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - mapPartitions
object Spark02_RDD_Transform_MapPartitions {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>>>>")
      iter.map(_ * 2)
    })

    mapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}