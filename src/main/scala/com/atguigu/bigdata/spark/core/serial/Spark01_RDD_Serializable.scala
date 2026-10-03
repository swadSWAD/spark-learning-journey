package com.atguigu.bigdata.spark.core.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 规则：算子内用到的外部对象，必须可序列化
object Spark01_RDD_Serializable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Serial")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))

    // 想要在算子里使用的外部对象 → 必须 extends Serializable
    val user = new User()

    // 算子在Executor执行，需要把user发过去
    val resultRDD = rdd.map(
      word => user.insert(word)
    )

    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

// 必须加 extends Serializable，否则报错！
class User extends Serializable {
  def insert(word: String): String = {
    "insert:" + word
  }
}