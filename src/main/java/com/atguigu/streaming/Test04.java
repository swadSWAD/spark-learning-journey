package com.atguigu.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Test04 {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建配置
        SparkConf conf = new SparkConf()
                .setAppName("KafkaWordCount")
                .setMaster("local[*]");

        // 2. 创建StreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        ssc.checkpoint("cp");

        // ===================== 修复这里！使用 Queue 而不是 List =====================
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(ssc.sparkContext().parallelize(Arrays.asList("hello spark", "hello kafka")));
        queue.add(ssc.sparkContext().parallelize(Arrays.asList("hello spark", "hello hadoop")));

        JavaDStream<String> lines = ssc.queueStream(queue, true);

        // 单词统计
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> result = wordAndOne.reduceByKey(Integer::sum);

        result.print();

        ssc.start();
        ssc.awaitTermination();
    }
}