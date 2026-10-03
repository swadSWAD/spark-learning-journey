package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - glom
object Spark05_RDD_Transform_Glom {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - glom
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    // TODO 关闭环境
    sc.stop()
  }
}
