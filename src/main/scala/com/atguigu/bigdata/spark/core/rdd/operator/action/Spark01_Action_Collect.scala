package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Action_Collect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val array = rdd.collect()
    array.foreach(println)
    sc.stop()
  }
}