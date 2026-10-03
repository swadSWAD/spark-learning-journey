package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 3.1.1 CSV 文件读写
public class Test08_CSV {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 读取 CSV（带表头）
        Dataset<Row> csvDF = spark.read()
                .option("header", "true")
                .option("sep", ",")
                .csv("input/user.csv");

        System.out.println("===== 读取 CSV =====");
        csvDF.show();
        csvDF.printSchema();

        // 写出 CSV
        csvDF.write()
                .mode("overwrite")
                .option("header", "true")
                .csv("output/csv");

        spark.close();
    }
}