package com.atguigu.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Test02_Window {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建环境
        SparkConf conf = new SparkConf()
                .setAppName("Test02_Window")
                .setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(3000));

        // 2. 创建 RDD 队列（修复：必须用 Queue！）
        Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
        rddQueue.add(ssc.sparkContext().parallelize(Arrays.asList("hello spark", "hello streaming")));
        rddQueue.add(ssc.sparkContext().parallelize(Arrays.asList("hello world", "spark spark")));
        rddQueue.add(ssc.sparkContext().parallelize(Arrays.asList("streaming window", "hello test")));

        // 3. 从队列创建流（100% 不报错）
        JavaDStream<String> lineStream = ssc.queueStream(rddQueue, true);

        // 4. 切分单词
        JavaDStream<String> wordStream = lineStream.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );

        // ==================== 窗口操作 ====================
        JavaDStream<String> windowStream = wordStream.window(
                new Duration(6000),   // 窗口长度 6秒
                new Duration(3000)    // 滑动间隔 3秒
        );

        // 5. 统计单词
        windowStream
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .print();

        // 6. 启动
        ssc.start();
        ssc.awaitTermination();
    }
}