package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - filter
object Spark07_RDD_Transform_Filter {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 保留偶数
    val filterRDD: RDD[Int] = rdd.filter(_ % 2 == 0)

    filterRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}