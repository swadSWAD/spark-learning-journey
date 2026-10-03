package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - sortBy
object Spark12_RDD_Transform_SortBy {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sortBy
    val rdd = sc.makeRDD(List(4, 5, 1, 3, 2))

    // 默认升序
    val sortRDD: RDD[Int] = rdd.sortBy(num => num)

    sortRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}