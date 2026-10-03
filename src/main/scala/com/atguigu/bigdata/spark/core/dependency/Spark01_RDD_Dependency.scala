package com.atguigu.bigdata.spark.core.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// RDD 依赖关系（血缘 + 宽窄依赖）
object Spark01_RDD_Dependency {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Dependency")
    val sc = new SparkContext(conf)

    // 1. 读取文件
    val lines: RDD[String] = sc.textFile("data/word.txt")

    // 2. 切分单词（窄依赖）
    val words = lines.flatMap(_.split(" "))

    // 3. 转成 (word,1)（窄依赖）
    val wordOne = words.map((_, 1))

    // 4. reduceByKey（宽依赖！）
    val result = wordOne.reduceByKey(_ + _)

    // 打印血缘关系（一看就懂）
    println("===== RDD 血缘关系 =====")
    println(result.toDebugString)

    result.collect()

    sc.stop()
  }
}