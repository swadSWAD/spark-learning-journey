# Spark 学习实战（spark-learning-journey）

从 RDD 算子到 SparkSQL、Structured Streaming 的**动手练习代码**。每个文件都是一个独立可运行的示例，覆盖日常开发中最常用的 API。

- **Scala** 写 Spark Core：RDD 算子、累加器、广播变量、分区器、持久化、序列化
- **Java** 写 SparkSQL 与 Streaming：DSL、UDF/UDAF/UDTF、多数据源读写、窗口计算

## 代码组织

### Spark Core（Scala，38 个文件）

| 目录 | 内容 | 文件数 |
|:---|:---|:---:|
| `core/rdd/builder/` | RDD 创建方式 + 12 个转换算子 | 14 |
| `core/rdd/operator/transform/` | 键值对算子（mapValues / groupByKey / reduceByKey / sortByKey） | 4 |
| `core/rdd/operator/action/` | 10 个行动算子（collect / count / take / aggregate / fold / saveAsTextFile 等） | 10 |
| `core/accumulator/` | 累加器 | 1 |
| `core/broadcast/` | 广播变量（大表 join 小表优化） | 1 |
| `core/partitioner/` | 自定义分区器 | 1 |
| `core/persist/` | RDD 持久化与缓存 | 1 |
| `core/dependency/` | 宽依赖与窄依赖 | 1 |
| `core/serial/` | 序列化与闭包检查 | 1 |
| `core/wc/` | WordCount 完整实现 | 1 |
| 根目录 | 综合练习（WordCount / 算子汇总 / SparkSQL 入门） | 3 |

### Spark SQL（Java，16 个文件）

| 文件 | 主题 |
|:---|:---|
| `Test01_Method.java` | DataFrame 创建方式 |
| `Test02_SQL.java` | SQL 方式（临时视图） |
| `Test03_DSL.java` | DSL 方式 |
| `Test04_UDF.java` / `Test13_UDF.java` | 自定义函数 UDF |
| `Test05_UDAF.java` / `Test14_UDAF.java` | 自定义聚合函数 UDAF |
| `Test06_UDTF_Replace.java` / `Test15_UDTF.java` | 自定义表生成函数 UDTF |
| `Test07_LoadSave.java` | 数据加载与保存 |
| `Test08_CSV.java` | CSV 数据源 |
| `Test09_Parquet.java` | Parquet 数据源 |
| `Test10_MySQL.java` | 读写 MySQL |
| `Test11_GenericLoadSave.java` | 通用加载保存 |
| `Test12_SparkSQLFunc.java` | 内置函数 |
| `Bean/User.java` | POJO 映射示例 |

### Spark Streaming（Java，4 个文件）

| 文件 | 主题 |
|:---|:---|
| `Test01_HelloWorld.java` | 第一个 Streaming 程序 |
| `Test02_Window.java` | 窗口操作 |
| `Test03_UpdateState.java` | UpdateStateByKey 状态更新 |
| `Test04.java` | 与 Kafka 集成 |

## 环境要求

| 组件 | 版本 |
|:---|:---|
| JDK | 1.8 |
| Scala | 2.12.15 |
| Spark | 3.3.1 |
| Maven | 3.6+（可选，IDEA 内置也可以） |

## 如何运行

### 方式一：IDEA（推荐）

直接用 IDEA 打开本目录，等待 Maven 依赖下载完成后，右键任意 `main` 方法运行即可。

> 所有示例都用了 `setMaster("local[*]")` 本地模式，不依赖集群。

### 方式二：Maven 命令行

```bash
# 编译
mvn clean compile

# 运行某个示例（需先安装 exec 插件，或用 IDEA 运行）
```

### 方式三：提交到集群

```bash
mvn clean package
spark-submit --class com.atguigu.bigdata.spark.core.wc.Spark01_WordCount \
    --master yarn \
    target/spark-learning-1.0.0.jar
```

## 依赖 scope 说明

pom.xml 里 Spark 相关依赖的 scope 都是 **`provided`**：

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>3.3.1</version>
    <scope>provided</scope>
</dependency>
```

**为什么要这样**：提交任务时集群的 `$SPARK_HOME/jars` 下已经有这些包，打进自己的 jar 里既浪费空间，又可能和集群版本冲突。

**会不会影响本地运行**：不会。`provided` 在编译期和测试期都可见，IDEA 里直接跑 `main()` 完全正常。

## 数据文件

```
data/word.txt        # WordCount 输入（4 行）
input/word.txt       # 备用输入
input/user.json      # SparkSQL 读取 JSON
input/user.csv       # SparkSQL 读取 CSV
```

## 知识点速查

### RDD 算子分类

| 类型 | 特点 | 示例 |
|:---|:---|:---|
| **转换算子 Transform** | 惰性执行，返回新 RDD | `map` `flatMap` `filter` `reduceByKey` |
| **行动算子 Action** | 触发实际计算 | `collect` `count` `take` `saveAsTextFile` |

### 两个高频考点

**`reduceByKey` 和 `groupByKey` 的区别**

`reduceByKey` 会在 **Map 端先做局部聚合**（类似 Combiner），再发往 Reduce 端；`groupByKey` 把所有 value 直接发到 Reduce 端才聚合。数据量大时 `groupByKey` 容易 OOM，**能用 `reduceByKey` 就不要用 `groupByKey`**。

**广播变量为什么能优化 join**

小表直接用变量参与 join 时，每个 Task 都要复制一份，网络和内存开销大。用 `sc.broadcast()` 广播后，**每个 Executor 只存一份**，Task 共享读取。

详细内容见 [docs](docs) 目录。

## License

MIT
