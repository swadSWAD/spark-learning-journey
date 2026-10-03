package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Key-Value 算子：groupByKey
object Spark15_RDD_Transform_GroupByKey {
  def main(args: Array[String]): Unit = {

    // 1. 环境准备
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 2. 创建 Key-Value 类型 RDD
    val rdd = sc.makeRDD(List(
      ("a", 1),
      ("a", 2),
      ("b", 3),
      ("b", 4)
    ))

    // 3. groupByKey：按 key 分组
    // 相同 key 的 value 放到一个迭代器中
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    // 4. 打印结果
    groupRDD.collect().foreach(println)

    // 5. 关闭环境
    sc.stop()
  }
}