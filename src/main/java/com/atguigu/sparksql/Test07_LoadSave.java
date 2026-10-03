package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 第3章 数据加载与保存
public class Test07_LoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 1. 读取 JSON 文件
        Dataset<Row> df = spark.read().json("input/user.json");
        System.out.println("===== 读取 JSON =====");
        df.show();

        // 2. 保存为 JSON
        df.write()
                .mode("overwrite")  // 覆盖模式
                .json("output/json");

        // 3. 保存为 Parquet（Spark默认格式）
        df.write()
                .mode("overwrite")
                .parquet("output/parquet");

        // 4. 读取 Parquet
        Dataset<Row> parquetDF = spark.read().parquet("output/parquet");
        System.out.println("===== 读取 Parquet =====");
        parquetDF.show();

        spark.close();
    }
}