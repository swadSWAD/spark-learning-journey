package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

// 2.2.3 DSL 方式
public class Test03_DSL {
    public static void main(String[] args) {
        // 1. 创建配置
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        // 2. 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 3. 读取 json
        Dataset<Row> lineRDD = spark.read().json("input/user.json");

        // 4. DSL 语法：查询、过滤、改名
        Dataset<Row> result = lineRDD.select(
                col("name").as("newName"),    // 改名
                col("age").plus(1).as("newAge") // age +1
        ).filter(col("age").gt(18));      // 过滤 age>18

        // 5. 展示
        result.show();

        // 6. 关闭
        spark.close();
    }
}