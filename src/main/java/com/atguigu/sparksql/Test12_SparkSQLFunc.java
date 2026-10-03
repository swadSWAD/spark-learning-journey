package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Test12_SparkSQLFunc {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 读取已有数据
        Dataset<Row> df = spark.read().json("input/user.json");

        System.out.println("===== 原始数据 =====");
        df.show();

        // 1. 统计行数
        df.select(count("*")).show();

        // 2. 最大值、最小值、总和、平均值
        df.select(
                max("age"),
                min("age"),
                sum("age"),
                avg("age")
        ).show();

        // 3. 分组统计
        df.groupBy("age").count().show();

        spark.close();
    }
}