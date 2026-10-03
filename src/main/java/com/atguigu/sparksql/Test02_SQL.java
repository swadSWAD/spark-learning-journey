package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 2.2.2 SQL 方式
public class Test02_SQL {
    public static void main(String[] args) {
        // 1. 创建配置
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        // 2. 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 3. 读取 json 文件
        Dataset<Row> lineDS = spark.read().json("input/user.json");

        // 4. 创建临时视图（关键！SQL 必须先建表）
        lineDS.createOrReplaceTempView("t1");

        // 5. 执行 SQL 查询
        Dataset<Row> result = spark.sql("select * from t1 where age > 18");

        // 6. 打印结果
        result.show();

        // 7. 关闭
        spark.close();
    }
}