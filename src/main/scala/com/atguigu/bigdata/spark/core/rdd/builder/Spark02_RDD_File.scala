package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 从文件创建 RDD
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 从文件创建 RDD
    // 路径默认以当前项目的根路径为基准，可以写绝对路径，也可以写相对路径
    // 路径可以是文件路径，也可以是目录路径
    // val rdd: RDD[String] = sc.textFile("datas/1.txt")
    val rdd: RDD[String] = sc.textFile("input")

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}