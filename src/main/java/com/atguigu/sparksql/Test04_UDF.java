package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

// 2.3 自定义函数 UDF
public class Test04_UDF {
    public static void main(String[] args) {
        // 1. 创建环境
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 2. 读取数据
        Dataset<Row> df = spark.read().json("input/user.json");
        df.createOrReplaceTempView("user");

        // 3. 注册 UDF 函数（重点！）
        // 函数名：addName
        // 功能：给名字前面加上 "Name:"
        spark.udf().register("addName", new UDF1<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "Name:" + name;
            }
        }, org.apache.spark.sql.types.DataTypes.StringType);

        // ========== 方式1：在 SQL 中使用 ==========
        System.out.println("===== SQL 方式使用 UDF =====");
        spark.sql("select name, addName(name) as new_name, age from user").show();

        // ========== 方式2：在 DSL 中使用 ==========
        System.out.println("===== DSL 方式使用 UDF =====");
        df.select(
                col("name"),
                callUDF("addName", col("name")).as("new_name"),
                col("age")
        ).show();

        // 关闭
        spark.close();
    }
}