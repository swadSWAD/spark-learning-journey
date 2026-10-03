package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 转换算子 - map
object Spark01_RDD_Transform_Map {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 1 * 2, 2 * 2 ...
    def mapFunction(num: Int): Int = {
      num * 2
    }

    //val mapRDD: RDD[Int] = rdd.map(mapFunction)
    //val mapRDD: RDD[Int] = rdd.map((num:Int)=>{num*2})
    //val mapRDD: RDD[Int] = rdd.map((num:Int)=>num*2)
    //val mapRDD: RDD[Int] = rdd.map((num)=>num*2)
    //val mapRDD: RDD[Int] = rdd.map(num=>num*2)
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}