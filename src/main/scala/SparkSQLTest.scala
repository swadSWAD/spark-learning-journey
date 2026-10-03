package com.atguigu.spark

import org.apache.spark.sql.SparkSession

object SparkSQLTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSQLTest")
      .master("local[*]")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 造数据
    val data = Seq(
      ("zhangsan", 20, "male"),
      ("lisi", 25, "male"),
      ("wangwu", 22, "female"),
      ("zhaoliu", 30, "male")
    ).toDF("name", "age", "gender")

    // 创建视图
    data.createOrReplaceTempView("t_user")

    // SQL 查询
    println("=== 全部用户 ===")
    spark.sql("select * from t_user").show()

    println("=== 男性 ===")
    spark.sql("select * from t_user where gender='male'").show()

    println("=== 统计 ===")
    spark.sql("select gender, count(*) from t_user group by gender").show()

    spark.stop()
  }
}