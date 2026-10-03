package com.atguigu.sparksql;

import com.atguigu.sparksql.Bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Test01_Method {
    public static void main(String[] args) {

        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //3. 编写代码
        // 按照行读取
        Dataset<Row> lineDS = spark.read().json("input/user.json");

        // 转换为类和对象
        Dataset<User> userDS = lineDS.as(Encoders.bean(User.class));

        // 函数式的方法
        Dataset<User> userDataset = lineDS.map(new MapFunction<Row, User>() {
            @Override
            public User call(Row value) throws Exception {
                return new User(value.getLong(0), value.getString(1));
            }
        }, Encoders.bean(User.class));

        // 常规方法
        Dataset<User> sortDS = userDataset.sort(new Column("age"));
        sortDS.show();

        // 区分
        RelationalGroupedDataset groupByDS = userDataset.groupBy("name");
        Dataset<Row> count = groupByDS.count();
        count.show();

        // 推荐使用函数式的方法
        KeyValueGroupedDataset<String, User> groupedDataset = userDataset.groupByKey(new MapFunction<User, String>() {
            @Override
            public String call(User value) throws Exception {
                return value.name;
            }
        }, Encoders.STRING());

        Dataset<Tuple2<String, User>> result = groupedDataset.reduceGroups(new ReduceFunction<User>() {
            @Override
            public User call(User v1, User v2) throws Exception {
                return new User(Math.max(v1.age, v2.age), v1.name);
            }
        });

        result.show();

        //4. 关闭sparkSession
        spark.close();
    }
}