package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Action_CountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3)))
    println(rdd.countByKey())
    sc.stop()
  }
}