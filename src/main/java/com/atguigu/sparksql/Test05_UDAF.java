package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import java.io.Serializable;
import static org.apache.spark.sql.functions.udaf;

// UDAF 强类型聚合函数 - 求平均年龄
public class Test05_UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        // 读取数据并创建视图
        spark.read().json("input/user.json").createOrReplaceTempView("user");

        // 注册 UDAF
        spark.udf().register("avgAge", udaf(new MyAvg(), Encoders.LONG()));

        // 使用 UDAF
        spark.sql("select avgAge(age) as average_age from user").show();

        spark.close();
    }

    // 中间缓存缓冲区
    public static class Buffer implements Serializable {
        private Long sum;
        private Long count;

        public Buffer() {}

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }

        public Long getSum() { return sum; }
        public void setSum(Long sum) { this.sum = sum; }
        public Long getCount() { return count; }
        public void setCount(Long count) { this.count = count; }
    }

    // 自定义 UDAF 聚合逻辑
    public static class MyAvg extends Aggregator<Long, Buffer, Double> {
        // 初始化缓冲区
        @Override
        public Buffer zero() {
            return new Buffer(0L, 0L);
        }

        // 分区内计算
        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setSum(b.getSum() + a);
            b.setCount(b.getCount() + 1);
            return b;
        }

        // 分区间合并
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        // 最终计算结果
        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }

        // 缓冲区编码器
        @Override
        public Encoder<Buffer> bufferEncoder() {
            return Encoders.kryo(Buffer.class);
        }

        // 输出编码器
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}