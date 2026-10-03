package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Action_Foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4))
    rdd.foreach(println)
    sc.stop()
  }
}