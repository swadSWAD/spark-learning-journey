package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - coalesce
object Spark10_RDD_Transform_Coalesce {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - coalesce
    val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

    // 缩减分区数：3 → 2
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)

    println("缩减后分区数：" + coalesceRDD.getNumPartitions)
    coalesceRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}