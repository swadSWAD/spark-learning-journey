package com.atguigu.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Test03_UpdateState {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Test03")
                .setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(3000));
        ssc.checkpoint("cp");

        // 模拟数据流
        Queue<JavaRDD<String>> queue = new LinkedList<>();
        queue.add(ssc.sparkContext().parallelize(Arrays.asList("hello", "hello", "spark")));
        queue.add(ssc.sparkContext().parallelize(Arrays.asList("hello", "streaming")));

        JavaDStream<String> lines = ssc.queueStream(queue, true);

        // 转换成 (word,1)
        JavaPairDStream<String, Integer> pairs = lines.mapToPair(word -> new Tuple2<>(word, 1));

        // ===================== 修复版：无泛型错误 =====================
        JavaPairDStream<String, Integer> result = pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> current, Optional<Integer> state) {
                        // 当前批次求和
                        int sum = 0;
                        for (Integer i : current) {
                            sum += i;
                        }
                        // 加上历史状态
                        if (state.isPresent()) {
                            sum += state.get();
                        }
                        return Optional.of(sum);
                    }
                }
        );

        result.print();

        ssc.start();
        ssc.awaitTermination();
    }
}