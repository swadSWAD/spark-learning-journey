package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// RDD 持久化：cache / persist / checkpoint
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Persist")
    val sc = new SparkContext(conf)

    // 1. 创建一个复杂的RDD（模拟耗时计算）
    val rdd = sc.makeRDD(List("hello spark", "hello scala"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))

    // 2. 持久化演示（重要！）
    // ------------ cache ------------
    // 底层默认：MEMORY_ONLY
    // mapRDD.cache()

    // ------------ persist ------------
    // 可以指定存储级别
    import org.apache.spark.storage.StorageLevel
    // mapRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // ------------ checkpoint ------------
    // 1. 必须设置检查点目录（通常是HDFS，本地模式也可以设本地路径）
    sc.setCheckpointDir("cp")

    // 2. 做检查点（会截断血缘，切分复杂RDD，只保留结果）
    mapRDD.checkpoint()

    // 3. 行动算子（触发计算）
    val result = mapRDD.collect()
    println("结果：" + result.mkString(","))

    sc.stop()
  }
}