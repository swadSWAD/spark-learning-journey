package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - sample
object Spark08_RDD_Transform_Sample {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sample
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 三个参数：
    // 1. 抽取数据后是否将数据返回给原数据池（放回/不放回）
    // 2. 每条数据被抽取的概率
    // 3. 随机数种子
    val sampleRDD: RDD[Int] = rdd.sample(false, 0.4, 1)

    sampleRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}