# 03 · Spark 调优

> 调优的前提是**先定位瓶颈**，而不是上来就改参数。盲目调参往往适得其反。

## 调优的基本思路

```
1. 跑一遍任务，打开 Spark UI
2. 看哪里慢：是某个 Stage 卡住？还是某个 Task 特别慢？还是 GC 时间过长？
3. 针对瓶颈改 1-2 个参数，重新跑，对比效果
4. 重复以上步骤
```

**核心原则**：一次只改一个参数，改完必须验证。同时改五个参数，效果好你不知道是哪个起作用，效果差你也不知道是哪个搞坏的。

## 一、资源调优

### 关键参数

| 参数 | 说明 | 建议 |
|:---|:---|:---|
| `spark.executor.instances` | Executor 数量 | 看队列可用资源 |
| `spark.executor.cores` | 每个 Executor 的核数 | **4~5 个** |
| `spark.executor.memory` | 每个 Executor 的堆内存 | 见下方计算 |
| `spark.driver.memory` | Driver 堆内存 | 默认 1G，`collect` 数据多时要调大 |
| `spark.executor.memoryOverhead` | 堆外内存（NIO、字符串等） | 默认 `max(384MB, executor.memory × 0.1)` |

### 核数为什么建议 4~5

一个 core 对应一个 Task 线程。核数太少，单个 Executor 并发上不去；核数太多（比如 16 个），多个 Task 同时读写 HDFS 会**争抢磁盘 IO**，反而整体变慢。

### 内存怎么算

```
集群可用内存 = 总内存 - 系统预留（约 20%）

每个 Executor 可用 ≈ 集群可用内存 / Executor 数量
spark.executor.memory ≈ 每个 Executor 可用 - memoryOverhead
```

**举例**：3 台机器各 16G，共 48G，YARN 可用约 40G，想开 6 个 Executor：

```
每个 Executor ≈ 40G / 6 ≈ 6.6G
memoryOverhead = max(384MB, 6.6G × 0.1) ≈ 660MB
spark.executor.memory ≈ 6G
```

### 完整的提交参数示例

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 6 \
  --executor-cores 4 \
  --executor-memory 6G \
  --driver-memory 2G \
  --conf spark.executor.memoryOverhead=1G \
  --class com.atguigu.bigdata.spark.core.wc.Spark01_WordCount \
  target/spark-learning-1.0.0.jar
```

## 二、内存模型

Spark 1.6 之后统一了内存管理，Executor 堆内存划分如下：

```
┌─────────────────────────────────────────────┐
│  堆内存（spark.executor.memory）              │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │ Reserved Memory: 固定 300MB            │  │
│  ├───────────────────────────────────────┤  │
│  │ User Memory: 40%                       │  │  ← 存用户自定义对象、RDD 元数据
│  │   = (堆 - 300MB) × (1 - 0.6)           │  │
│  ├───────────────────────────────────────┤  │
│  │ Spark Memory: 60%                      │  │
│  │   = (堆 - 300MB) × 0.6                 │  │
│  │                                        │  │
│  │  ┌──────────────┬──────────────────┐   │  │
│  │  │ Storage 50%  │  Execution 50%   │   │  │  ← 两者可互相借用
│  │  │ (cache/persist)│ (shuffle/join) │   │  │
│  │  └──────────────┴──────────────────┘   │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

| 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `spark.memory.fraction` | 0.6 | Spark Memory 占（堆 - 300MB）的比例 |
| `spark.memory.storageFraction` | 0.5 | Storage 占 Spark Memory 的比例 |

**关键点**：Storage 和 Execution **可以互相借用**。Execution 借用 Storage 后，如果 Storage 需要空间，会把借出去的部分挤掉（丢弃缓存块，需要时重新计算）。

**如果频繁丢缓存**：调大 `spark.executor.memory` 或调大 `spark.memory.fraction`。

## 三、序列化调优

Spark 默认用 **Java 序列化**，可以换成 **Kryo**：

```bash
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```

**提升**：Kryo 通常比 Java 序列化**快 10 倍左右，体积小 10 倍左右**。

如果自定义类较多，建议注册：

```scala
val conf = new SparkConf()
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(classOf[User], classOf[Order]))
```

**不注册会怎样**：Kryo 仍然能用，只是每个对象会额外写入类全名，体积变大。类很多时注册收益明显。

> 对应代码：`core/serial/Spark01_RDD_Serializable.scala`

## 四、并行度调优

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| `spark.default.parallelism` | Executor 总核数（YARN 模式） | RDD 的默认分区数 |
| `spark.sql.shuffle.partitions` | **200** | SparkSQL shuffle 后的分区数 |

### 并行度公式

```
理想并行度 ≈ Executor 总核数 × 2 ~ 3
```

乘 2~3 是为了让 CPU 有任务可切换，避免因 IO 等待造成空闲。

**举例**：6 个 Executor × 4 核 = 24 核，并行度设置 48~72 比较合适。

### spark.sql.shuffle.partitions 的坑

这个参数默认是 **200**，对小数据量来说是灾难——会启动 200 个 Task 处理几万条数据，**调度开销远大于计算开销**。

```scala
// 小数据量场景，调到实际需要的量级
spark.conf.set("spark.sql.shuffle.partitions", 10)
```

**判断依据**：如果 Spark UI 里大量 Task 的执行时间只有几十毫秒，说明分区数过多。

> 对应代码：`core/rdd/operator/action/saveAsTextFile.scala` 里能看到输出有 16 个 part 文件，就是因为分区数是 16。

## 五、Shuffle 调优

### 自适应执行（AQE，Spark 3.x 重点）

Spark 3.2 起 **AQE 默认开启**，能自动做三件事：

| 能力 | 参数 | 作用 |
|:---|:---|:---|
| 合并小分区 | `spark.sql.adaptive.coalescePartitions.enabled` | 把大量小分区合并，减少 Task 数 |
| 处理数据倾斜 | `spark.sql.adaptive.skewJoin.enabled` | 自动拆分倾斜的大分区 |
| 转换 join 策略 | `spark.sql.adaptive.localShuffleReader.enabled` | 小表广播，省掉 shuffle |

```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
```

**AQE 的价值**：以前这些都要人工预估参数，现在 Spark 根据**运行时统计信息**动态调整，对小集群尤其友好。

### 常规 shuffle 参数

| 参数 | 默认值 | 调整建议 |
|:---|:---:|:---|
| `spark.reducer.maxSizeInFlight` | 48MB | Reduce 端一次拉取的数据量，网络好可调到 96MB |
| `spark.shuffle.file.buffer` | 32KB | Map 端写缓冲区，可调到 64KB |
| `spark.shuffle.io.maxRetries` | 3 | 拉取失败重试次数，网络差调到 10 |
| `spark.shuffle.io.retryWait` | 5s | 重试间隔 |

**注意**：`maxSizeInFlight` 调大会增加 Reduce 端内存压力，不是越大越好。

## 六、数据倾斜处理（面试高频）

### 怎么发现

- Spark UI 里某个 Stage 大部分 Task 几秒完成，**个别 Task 跑几分钟**
- 或者任务卡在 99% 不动
- 或者报 `OutOfMemoryError`

### 根本原因

某个 key 的数据量远大于其他 key。常见来源：

- 大量 **null 值**（比如用户 ID 为空）
- **热点 key**（比如某个爆款商品被所有人访问）
- join 的两张表 key 分布严重不均

### 解决方案

#### 方案 1：过滤无效 key（最简单）

```scala
// 如果 null 值本身没意义，直接过滤掉
rdd.filter(_._1 != null).reduceByKey(_ + _)
```

#### 方案 2：加盐打散 + 两次聚合

```scala
// 第一次聚合：给 key 加随机前缀，把数据打散到多个分区
val salted = rdd.map { case (k, v) =>
  (k + "_" + Random.nextInt(10), v)
}.reduceByKey(_ + _)

// 第二次聚合：去掉前缀，再聚合一次
val result = salted.map { case (k, v) =>
  (k.split("_")(0), v)
}.reduceByKey(_ + _)
```

**原理**：原本一个 key 落到一个分区，加盐后变成 10 个 key 落到 10 个分区，并行处理。

#### 方案 3：用 broadcast join 代替 reduce join

```scala
// 小表广播出去，大表本地关联，完全避免 shuffle
val smallMap = sc.broadcast(smallRdd.collectAsMap())
val result = bigRdd.map { case (k, v) => (k, smallMap.value.getOrElse(k, null), v) }
```

**适用条件**：一张表足够小（能放进 Executor 内存），通常几百 MB 以内。

> 对应代码：`core/broadcast/Spark01_Broadcast.scala`

#### 方案 4：单独处理热点 key

把倾斜的 key 单独抽出来处理，剩下的走正常流程，最后 union。

### 方案对比

| 方案 | 适用场景 | 代价 |
|:---|:---|:---|
| 过滤无效 key | 有大量 null | 会丢数据，确认业务上可丢 |
| 加盐打散 | 热点 key 明确 | 要做两次聚合，代码复杂 |
| broadcast join | 一张表足够小 | 小表不能太大，否则 Executor OOM |
| 单独处理 | 少数几个热点 key | 要写两套逻辑 |

## 七、GC 调优

### 用 G1 垃圾回收器

```bash
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45"
```

G1 相比 CMS 的优势：可预测停顿时间，适合大堆内存。

### 减少 Full GC 的思路

Full GC 通常意味着**对象太多、内存不够**。治本的方法：

| 手段 | 说明 |
|:---|:---|
| 用 Kryo 序列化 | 减少对象数量和体积 |
| 用 `mapPartitions` 替代 `map` | 减少函数调用和临时对象 |
| 及时 `unpersist()` | 不用了的缓存及时释放 |
| 广播大变量 | 避免每个 Task 复制一份 |
| 用 `MEMORY_ONLY_SER` 缓存 | 序列化后存内存，省空间 |

### 怎么看 GC 情况

提交时加上：

```bash
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
```

**判断标准**：如果 GC 时间超过总运行时间的 10%，说明内存压力大，需要调优。

## 八、常见报错与排查

| 报错 | 原因 | 解决 |
|:---|:---|:---|
| `OutOfMemoryError: Java heap space` | Executor 堆内存不足 | 调大 `executor.memory`，或减少数据倾斜 |
| `Container killed by YARN for exceeding memory limits` | 超出 memoryOverhead | 调大 `spark.executor.memoryOverhead` |
| `GC overhead limit exceeded` | 频繁 Full GC | 用 Kryo、调大内存、减少对象 |
| `Shuffle file not found` | Executor 挂了，shuffle 数据丢失 | 调大内存避免 Executor 被杀，或开启 `spark.shuffle.service.enabled` |
| `NotSerializableException` | 算子引用了不可序列化的对象 | 用 `@transient` 标记，或改在 Executor 端重建 |
| `java.lang.IllegalArgumentException: requirement failed` | 自定义分区器返回值越界 | 检查 `getPartition` 返回值范围 |
| 任务卡在 99% | 数据倾斜 | 见第六节 |

### 排查顺序

```
1. 看 Spark UI 的 Stage 页面 → 哪个 Stage 慢
2. 看该 Stage 的 Task 时间分布 → 是否倾斜
3. 看 Executor 页面 → 是否有 Executor 频繁挂掉
4. 看 Environment 页面 → 参数是否生效
5. 看日志 → 具体报错
```

## 九、一个真实的小例子

本项目里 `saveAsTextFile` 的输出目录有 16 个 `part-00000` ~ `part-00015` 文件：

```
output/part-00000 ... part-00015
```

**原因**：RDD 的分区数是 16（`local[*]` 在本机有 8 核时，`makeRDD` 默认按核数分区），每个分区对应一个输出文件。

**如果分区数远大于数据量**：就会产生大量小文件，既浪费 HDFS 元数据空间，又拖慢后续读取。这时候应该用 `coalesce(1)` 或 `repartition(n)` 收拢分区。

```scala
// 收拢到 1 个分区再输出（小数据量场景）
rdd.coalesce(1).saveAsTextFile("output")
```

**注意**：`coalesce` 默认不 shuffle，只能减少分区；如果要从少变多，必须用 `repartition`，而且要加 `shuffle=true`。

> 对应代码：`core/rdd/operator/action/saveAsTextFile.scala`、`core/rdd/builder/Spark10_RDD_Transform_Coalesce.scala`

## 十、调参速查表

| 场景 | 参数 | 建议值 |
|:---|:---|:---|
| Executor 核数 | `spark.executor.cores` | 4~5 |
| 并行度 | `spark.default.parallelism` | 总核数 × 2~3 |
| SQL 分区数 | `spark.sql.shuffle.partitions` | 按数据量调，别用默认 200 |
| 序列化 | `spark.serializer` | Kryo |
| 自适应执行 | `spark.sql.adaptive.enabled` | true（Spark 3.2+ 默认开） |
| 倾斜 join | `spark.sql.adaptive.skewJoin.enabled` | true |
| 堆外内存 | `spark.executor.memoryOverhead` | max(384MB, 内存 × 0.1) |
| GC | `-XX:+UseG1GC` | 大堆内存场景 |
| Reduce 拉取 | `spark.reducer.maxSizeInFlight` | 48MB ~ 96MB |
| Map 写缓冲 | `spark.shuffle.file.buffer` | 32KB ~ 64KB |

## 最后

调优不是背参数，而是**理解每个参数在解决什么问题**。面试时如果能说清楚：

> "我遇到过数据倾斜，表现是 Stage 卡在 99%，通过 Spark UI 看到某个 Task 处理的数据量是其他 Task 的几十倍。原因是 join 的 key 里有大量 null 值，我先过滤掉 null，然后对剩余的倾斜 key 做了加盐打散，任务时间从 40 分钟降到 6 分钟。"

—— 这比背出十个参数名称有用得多。
