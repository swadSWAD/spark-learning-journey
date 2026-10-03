package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;

public class Test10_MySQL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 1. 读取本地 json
        Dataset<Row> df = spark.read().json("input/user.json");

        // 2. MySQL 连接信息
        Properties prop = new Properties();
        prop.setProperty("user", "root");
        prop.setProperty("password", "你的MySQL密码");
        String url = "jdbc:mysql://localhost:3306/test?useSSL=false&characterEncoding=utf8";

        // 3. 写入 MySQL（表会自动创建）
        df.write()
                .mode("overwrite")
                .jdbc(url, "user_spark", prop);

        // 4. 从 MySQL 读回来
        Dataset<Row> mysqlDF = spark.read()
                .jdbc(url, "user_spark", prop);

        System.out.println("===== 从 MySQL 读取的数据 =====");
        mysqlDF.show();

        spark.close();
    }
}