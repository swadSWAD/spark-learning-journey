package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Test14_UDAF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparksql")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df = spark.read().json("input/user.json");
        df.createOrReplaceTempView("user");

        spark.udf().register("myAvg", new MyAvg());

        spark.sql("select myAvg(age) as avg_age from user").show();

        spark.close();
    }
}

class MyAvg extends UserDefinedAggregateFunction {

    // 输入数据类型
    @Override
    public StructType inputSchema() {
        return new StructType()
                .add("age", DataTypes.LongType);
    }

    // 缓冲区数据类型
    @Override
    public StructType bufferSchema() {
        return new StructType()
                .add("sum", DataTypes.LongType)
                .add("count", DataTypes.LongType);
    }

    // 返回值类型
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    // 初始化
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
        buffer.update(1, 0L);
    }

    // 更新
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            long age = input.getLong(0);
            buffer.update(0, buffer.getLong(0) + age);
            buffer.update(1, buffer.getLong(1) + 1L);
        }
    }

    // 合并
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

    // 最终结果
    @Override
    public Object evaluate(Row buffer) {
        long sum = buffer.getLong(0);
        long count = buffer.getLong(1);
        return count == 0 ? 0.0 : (double) sum / count;
    }
}