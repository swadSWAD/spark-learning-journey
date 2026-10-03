package com.atguigu.bigdata.spark.core.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

// 键值对 RDD 数据分区 - 分区器
object Spark01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Partitioner")
    val sc = new SparkContext(conf)

    // 1. 创建 Key-Value RDD
    val rdd = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4),
      ("a", 5)
    ))

    // 查看原来的分区数
    println("分区前：" + rdd.partitions.length)

    // 2. 使用 Hash 分区器，分成 2 个区
    val partitionRDD: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))

    // 保存到文件，你能看到生成 2 个分区文件
    partitionRDD.saveAsTextFile("output-partitioner")

    println("分区后：" + partitionRDD.partitions.length)

    sc.stop()
  }
}