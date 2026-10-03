package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 尚硅谷SparkCore经典案例：WordCount
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // ==============================
    // 1. 创建Spark环境（固定模板）
    // ==============================
    val conf = new SparkConf()
      .setMaster("local[*]")    // 本地模式，*表示用所有CPU核心
      .setAppName("WordCount")  // 任务名称

    val sc = new SparkContext(conf) // Spark核心入口


    // ==============================
    // 2. 读取文件（一行一行读取）
    // ==============================
    val lines: RDD[String] = sc.textFile("data/word.txt")


    // ==============================
    // 3. 切分单词（扁平化）
    // hello spark → 拆成 hello、spark 两个单词
    // ==============================
    val words: RDD[String] = lines.flatMap(line => line.split(" "))


    // ==============================
    // 4. 转换成 (单词, 1) 格式
    // hello → (hello,1)
    // ==============================
    val wordAndOne: RDD[(String, Int)] = words.map(word => (word, 1))


    // ==============================
    // 5. 按key分组、求和（核心！）
    // (hello,1) + (hello,1) → (hello,2)
    // ==============================
    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey((v1, v2) => v1 + v2)


    // ==============================
    // 6. 把结果收集到本地打印
    // ==============================
    val result: Array[(String, Int)] = wordAndCount.collect()
    result.foreach(println)


    // ==============================
    // 7. 关闭环境
    // ==============================
    sc.stop()
  }
}