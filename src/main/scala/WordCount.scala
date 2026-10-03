package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    // 1. 创建 Spark 入口
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[2]") // 本地运行，集群会自动覆盖

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") // 减少日志

    // 2. 读取文件（HDFS 路径）
    val lines = sc.textFile("hdfs://hadoop102:8020/input/word.txt")

    // 3. 核心 WordCount 逻辑
    val result = lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // 4. 打印结果
    result.foreach(println)

    // 5. 保存到 HDFS
    result.saveAsTextFile("hdfs://hadoop102:8020/tmp/wc_result")

    // 6. 关闭
    sc.stop()
  }
}