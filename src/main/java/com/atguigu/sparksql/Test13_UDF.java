package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

// UDF 自定义函数（修复类型：Long → Long）
public class Test13_UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 读取数据
        Dataset<Row> df = spark.read().json("input/user.json");
        df.createOrReplaceTempView("user");

        // 注册 UDF：全部改成 Long 类型！
        spark.udf().register("addAge", new UDF1<Long, Long>() {
            @Override
            public Long call(Long age) {
                return age == null ? 0L : age + 10L;
            }
        }, DataTypes.LongType);

        // 使用 UDF
        System.out.println("===== 使用自定义UDF =====");
        spark.sql("select name, age, addAge(age) as new_age from user").show();

        spark.close();
    }
}