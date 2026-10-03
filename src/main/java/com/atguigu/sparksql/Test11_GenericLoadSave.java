package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 通用的加载和保存 API
public class Test11_GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // ============ 通用读取：默认是 parquet ============
        Dataset<Row> df = spark.read().load("output/parquet");

        System.out.println("===== 通用方式读取 Parquet ======");
        df.show();

        // ============ 通用保存：默认是 parquet ============
        df.write().mode("overwrite").save("output/generic_save");

        System.out.println("===== 已保存到 output/generic_save =====");

        spark.close();
    }
}