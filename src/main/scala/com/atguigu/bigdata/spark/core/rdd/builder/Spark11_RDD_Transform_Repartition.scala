package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - repartition
object Spark11_RDD_Transform_Repartition {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - repartition
    val rdd = sc.makeRDD(List(1,2,3,4,5,6), 2)

    // 增加分区：2 → 3
    val repartitionRDD: RDD[Int] = rdd.repartition(3)

    println("分区数：" + repartitionRDD.getNumPartitions)
    repartitionRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
