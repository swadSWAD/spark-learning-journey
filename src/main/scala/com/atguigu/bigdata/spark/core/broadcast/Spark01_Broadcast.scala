package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

// 广播变量：分布式共享只读变量
object Spark01_Broadcast {
  def main(args: Array[String] = Array()): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Broadcast")
    val sc = new SparkContext(conf)

    // 1. 定义一个要广播的集合（大数据）
    val map = mutable.Map(("a", 1), ("b", 2), ("c", 3))

    // 2. 广播出去
    val bc = sc.broadcast(map)

    val rdd = sc.makeRDD(List("a", "b", "c"))

    // 3. 在Executor端使用广播变量
    rdd.map(word => {
      // 获取广播变量的值
      bc.value.getOrElse(word, 0)
    }).collect().foreach(println)

    sc.stop()
  }
}