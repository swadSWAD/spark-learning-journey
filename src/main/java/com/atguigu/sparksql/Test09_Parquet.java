package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 3.1.3 Parquet 列式存储
public class Test09_Parquet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 1. 读取 json
        Dataset<Row> df = spark.read().json("input/user.json");

        // 2. 写出为 parquet
        df.write()
                .mode("overwrite")
                .parquet("output/parquet");

        // 3. 读取 parquet
        Dataset<Row> parquetDF = spark.read().parquet("output/parquet");

        System.out.println("===== 读取 Parquet =====");
        parquetDF.show();
        parquetDF.printSchema();

        spark.close();
    }
}