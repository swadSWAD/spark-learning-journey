package com.atguigu.bigdata.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

// 累加器：分布式累加
object Spark01_Accumulator {
  def main(args: Array[String] = Array()): Unit = {

    // 1. 环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 2. 创建累加器
    val sumAcc = sc.longAccumulator("sum")

    // 3. 在Executor端累加
    rdd.foreach(num => {
      sumAcc.add(num)
    })

    // 4. Driver端获取累加结果
    println("累加结果 = " + sumAcc.value)

    sc.stop()
  }
}