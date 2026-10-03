package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import java.util.Arrays;

public class Test15_UDTF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 注册 UDTF（Spark 3.x 用 UDF 返回数组实现炸裂）
        spark.udf().register("mySplit", new UDF1<String, String[]>() {
            @Override
            public String[] call(String s) {
                return s == null ? new String[0] : s.split(" ");
            }
        }, DataTypes.createArrayType(DataTypes.StringType));

        // 造测试数据
        Dataset<Row> df = spark.sql("select 'hello world' as str union all select 'spark sql' as str");
        df.createOrReplaceTempView("t1");

        // 使用 UDTF
        spark.sql("select explode(mySplit(str)) as word from t1").show();

        spark.close();
    }
}