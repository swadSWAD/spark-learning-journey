# 02 · Spark SQL

## 是什么

Spark SQL 是 Spark 处理**结构化数据**的模块，把 SQL 语句翻译成 RDD 作业执行。

它有三个核心抽象：

| 抽象 | 说明 | 类型安全 |
|:---|:---|:---:|
| **RDD** | 无 Schema，面向对象 | 是 |
| **DataFrame** | 有 Schema，本质是 `Dataset[Row]` | 否 |
| **Dataset** | 有 Schema，强类型 | 是 |

**为什么 DataFrame 比 RDD 快**：DataFrame 知道字段名和类型，Catalyst 优化器可以做**谓词下推、列裁剪、常量折叠**等优化；RDD 里是黑盒函数，优化器无从下手。

## 三种编程方式

### 1. SQL 方式

```java
Dataset<Row> df = spark.read().json("input/user.json");
df.createOrReplaceTempView("t1");           // 必须先建临时视图
Dataset<Row> result = spark.sql("select * from t1 where age > 18");
result.show();
```

### 2. DSL 方式

```java
Dataset<Row> result = df.select("name", "age")
                        .filter("age > 18")
                        .orderBy(df.col("age").desc());
```

### 3. 编程方式（RDD ↔ DataFrame）

```java
// RDD 转 DataFrame，需要定义 Schema
StructType schema = new StructType()
        .add("name", "string")
        .add("age", "int");
Dataset<Row> df = spark.createDataFrame(rdd, schema);
```

## DataFrame 的创建方式

| 方式 | 代码 |
|:---|:---|
| 从 JSON | `spark.read().json("path")` |
| 从 CSV | `spark.read().option("header", true).csv("path")` |
| 从 Parquet | `spark.read().parquet("path")` |
| 从 MySQL | `spark.read().format("jdbc").options(options).load()` |
| 从 RDD | `spark.createDataFrame(rdd, schema)` |

## 自定义函数

SparkSQL 提供三种自定义函数，区别在于**输入和输出的行数**：

| 类型 | 全称 | 输入 → 输出 | 用法 |
|:---|:---|:---|:---|
| **UDF** | User Defined Function | 一行 → 一个值 | `select myUdf(col("name"))` |
| **UDAF** | User Defined Aggregate Function | 多行 → 一个值 | `select myUdaf(col("age"))` |
| **UDTF** | User Defined Table Function | 一行 → 多行多列 | `select myUdtf(col("name"))` |

### UDF 示例（Java）

```java
import static org.apache.spark.sql.functions.udf;

UDF1<String, String> upper = name -> name.toUpperCase();
spark.udf().register("toUpper", upper, DataTypes.StringType);
spark.sql("select toUpper(name) from t1").show();
```

### UDAF 示例（Java，`UserDefinedAggregateFunction`）

需要实现 8 个方法：`inputSchema`、`bufferSchema`、`dataType`、`deterministic`、`initialize`、`update`、`merge`、`evaluate`。

| 方法 | 作用 |
|:---|:---|
| `inputSchema` | 输入字段的类型 |
| `bufferSchema` | 中间缓冲区的类型 |
| `initialize` | 初始化缓冲区 |
| `update` | 每条数据进来时更新缓冲区 |
| `merge` | 合并多个分区（Executor）的缓冲区 |
| `evaluate` | 从缓冲区算出最终结果 |

**核心思路**：`update` 是分区内逐条累加，`merge` 是分区之间合并，最后 `evaluate` 出结果。

### UDTF 示例

输入一行返回多行多列，需要实现 `process(Object[] args)` 方法，内部用 `forward()` 输出每一行。

## 数据源读写

### 通用写法

```java
// 读
spark.read().format("json").load("path");

// 写
df.write().format("json").mode(SaveMode.Overwrite).save("path");
```

`format` 支持：`json`、`csv`、`parquet`、`orc`、`jdbc`、`text`。

### 写入模式

| 模式 | 说明 |
|:---|:---|
| `Append` | 追加（默认） |
| `Overwrite` | 覆盖 |
| `ErrorIfExists` | 存在就报错 |
| `Ignore` | 存在就忽略 |

### 读写 MySQL

```java
Dataset<Row> df = spark.read()
        .format("jdbc")
        .option("url", "jdbc:mysql://hadoop102:3306/test")
        .option("dbtable", "user_info")
        .option("user", "root")
        .option("password", "******")
        .load();

df.write()
  .format("jdbc")
  .option("url", "jdbc:mysql://hadoop102:3306/test")
  .option("dbtable", "user_info_copy")
  .mode(SaveMode.Overwrite)
  .save();
```

> **注意**：写 MySQL 时 `mode(Overwrite)` 的行为是**先 DROP TABLE 再 CREATE**，不是清空数据。如果表有约束或索引，会被一起删掉。

## 与 Hive 集成

```java
SparkSession spark = SparkSession.builder()
        .appName("hive")
        .enableHiveSupport()        // 关键：开启 Hive 支持
        .getOrCreate();

spark.sql("select * from gmall.user_info").show();
```

需要在 classpath 中有 `hive-site.xml`，并且 Spark 编译时带了 Hive 支持（官方发行版默认带）。
