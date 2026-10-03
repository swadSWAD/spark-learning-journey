package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 尚硅谷 Spark 教程
// RDD 的创建
object Spark01_RDD_Create {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 创建 RDD
    // 1. 从内存中创建 RDD，将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq(1,2,3,4)
    // parallelize：并行
    // val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRDD 的底层逻辑就是 parallelize
    val rdd: RDD[Int] = sc.makeRDD(seq)

    // 打印
    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}