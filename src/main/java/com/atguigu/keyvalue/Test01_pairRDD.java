package com.atguigu.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Test01_pairRDD{
    public static void main(String[] args) {

        // 1. 创建环境
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("sparkCore");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. 普通 RDD
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4),2);

        // 3. 转为 Key-Value RDD（关键！）
        JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(
                new PairFunction<Integer, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                        // 把数字变成 (数字, 数字)
                        return new Tuple2<>(integer, integer);
                    }
                }
        );

        // 打印
        pairRDD.collect().forEach(System.out::println);

        sc.close();
    }
}