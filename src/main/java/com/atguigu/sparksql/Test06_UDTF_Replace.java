package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class Test06_UDTF_Replace {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 构造数据（完全避开 Row.create 错误）
        List<String> data = Arrays.asList(
                "hello,spark,sql",
                "java,python,scala"
        );
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        // 转成 DataFrame，列名叫 line
        Dataset<Row> source = ds.toDF("line");

        System.out.println("===== 原始数据 =====");
        source.show();

        // flatMap 实现 UDTF
        Dataset<String> result = source.flatMap(
                (FlatMapFunction<Row, String>) row ->
                        Arrays.asList(row.getString(0).split(",")).iterator(),
                Encoders.STRING()
        );

        System.out.println("===== 拆分后（模拟 UDTF） =====");
        result.show();

        spark.close();
    }
}