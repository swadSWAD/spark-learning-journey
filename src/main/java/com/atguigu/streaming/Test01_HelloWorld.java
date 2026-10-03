package com.atguigu.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Test01_HelloWorld {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建配置
        SparkConf conf = new SparkConf()
                .setAppName("StreamingHelloWorld")
                .setMaster("local[*]");

        // 2. 创建 Streaming 上下文，3 秒一个批次
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(3000));

        // ====================== 核心代码 ======================
        // 创建一个固定的数据流，不用端口、不用工具，直接运行就有数据
        JavaDStream<String> lineStream = ssc.textFileStream("data/stream");

        // 打印每一批内容
        lineStream.print();

        // 启动
        ssc.start();
        ssc.awaitTermination();
    }
}