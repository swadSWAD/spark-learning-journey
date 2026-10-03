# 01 · Spark Core

## Spark 是什么

基于**内存**的分布式计算框架。相比 MapReduce 每次都要落盘，Spark 把中间结果缓存在内存里，迭代计算场景下能快一个数量级。

## 四大特性

| 特性 | 说明 |
|:---|:---|
| 速度快 | 内存计算 + DAG 优化 |
| 易用 | 支持 Scala / Java / Python / R |
| 通用 | 一套引擎覆盖批处理、SQL、流处理、机器学习 |
| 兼容 | 可跑在 YARN / Mesos / K8s / Standalone 上 |

## 运行架构

```
        ┌──────────────┐
        │    Driver    │  运行 main()，创建 SparkContext
        │  (客户端)     │  划分 Stage、调度 Task
        └──────┬───────┘
               │
        ┌──────┴───────┐
        │  Cluster     │  申请资源（YARN 模式下是 RM）
        │  Manager     │
        └──────┬───────┘
               │
      ┌────────┼────────┐
      ↓        ↓        ↓
 ┌─────────┐┌─────────┐┌─────────┐
 │Executor ││Executor ││Executor │  真正执行 Task
 │  (102)  ││  (103)  ││  (104)  │  每个 Executor 是一个 JVM 进程
 └─────────┘└─────────┘└─────────┘
```

| 角色 | 职责 |
|:---|:---|
| **Driver** | 运行 `main()`、创建 `SparkContext`、把作业拆成 Stage 和 Task、调度执行 |
| **Executor** | 在 Worker 节点上执行 Task，缓存数据，一个 Executor 内有多个 Task 线程 |
| **Cluster Manager** | 分配资源（YARN 模式下就是 ResourceManager） |

## 核心概念

### RDD：弹性分布式数据集

RDD 是 Spark 最基本的数据抽象，有四个关键特性：

1. **分区**：数据被切成多个 partition，分布在不同的 Executor 上
2. **只读**：不能修改，只能通过转换算子生成新 RDD
3. **依赖**：记录与父 RDD 的血缘关系，用于容错
4. **惰性**：转换算子不会立即执行，只有遇到行动算子才真正计算

### 算子的两种类型

| 类型 | 特点 | 常见算子 |
|:---|:---|:---|
| **Transformation**（转换） | 惰性，返回新 RDD | `map` `flatMap` `filter` `groupBy` `reduceByKey` |
| **Action**（行动） | 触发计算，返回结果或写文件 | `collect` `count` `take` `first` `saveAsTextFile` |

**为什么要惰性**：Spark 可以在真正执行前对整个算子链做优化（合并、谓词下推、调整分区），如果每个算子立即执行就失去了优化空间。

## 常用转换算子

| 算子 | 作用 | 是否 shuffle |
|:---|:---|:---:|
| `map` | 一对一转换 | 否 |
| `mapPartitions` | 以分区为单位处理，比 map 高效（减少函数调用） | 否 |
| `flatMap` | 一对多展开 | 否 |
| `filter` | 过滤 | 否 |
| `distinct` | 去重 | **是** |
| `coalesce` | 缩减分区，默认不 shuffle | 否 |
| `repartition` | 重新分区，一定 shuffle | **是** |
| `sortBy` | 排序 | **是** |
| `groupBy` | 分组 | **是** |
| `reduceByKey` | 按 key 聚合 | **是**（但 Map 端预聚合） |
| `groupByKey` | 按 key 分组 | **是**（无预聚合） |

### reduceByKey vs groupByKey（面试高频）

```scala
// 推荐
rdd.reduceByKey(_ + _)

// 不推荐，数据量大时容易 OOM
rdd.groupByKey().mapValues(_.sum)
```

**区别**：`reduceByKey` 在 Map 端先做局部聚合，只把聚合结果发给 Reduce 端；`groupByKey` 把同一个 key 的所有 value 都发到 Reduce 端才聚合。

**结论**：能聚合的场景一律用 `reduceByKey`。

### map vs mapPartitions

```scala
// map：每条数据调用一次函数
rdd.map(x => x * 2)

// mapPartitions：每个分区调用一次函数，内部遍历
rdd.mapPartitions(iter => iter.map(_ * 2))
```

`mapPartitions` 减少了函数调用次数，但**一次处理整个分区的数据**，分区过大会 OOM。适合需要建立外部连接（如数据库连接）的场景——一个分区建一次连接，而不是每条数据建一次。

## 常用行动算子

| 算子 | 作用 |
|:---|:---|
| `collect` | 把结果全部拉回 Driver（数据不能太大） |
| `count` | 统计元素个数 |
| `first` | 取第一个元素 |
| `take(n)` | 取前 n 个 |
| `takeOrdered(n)` | 取排序后的前 n 个 |
| `aggregate` | 聚合，可指定初始值和分区内/分区间的聚合函数 |
| `fold` | 带初始值的聚合，要求分区内外函数一致 |
| `foreach` | 遍历（在 Executor 端执行，不会返回 Driver） |
| `countByKey` | 按 key 统计数量 |
| `saveAsTextFile` | 保存到文件 |

> **注意**：`foreach` 在 Executor 上执行，所以里面打印的内容出现在 Executor 日志里，而不是 Driver 的控制台。

## RDD 持久化

```scala
rdd.cache()      // 等价于 persist(StorageLevel.MEMORY_ONLY)
rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.unpersist()  // 释放缓存
```

**为什么要持久化**：RDD 是惰性的，每次遇到行动算子都会从头重算。如果多个行动算子用同一个 RDD，不缓存就会重复计算。

| 存储级别 | 说明 |
|:---|:---|
| `MEMORY_ONLY` | 只存内存，放不下就丢弃（默认） |
| `MEMORY_AND_DISK` | 内存放不下溢写到磁盘 |
| `DISK_ONLY` | 只存磁盘 |
| `MEMORY_ONLY_SER` | 序列化后存内存，省空间但读的时候要反序列化 |

## 宽依赖与窄依赖

| 类型 | 定义 | 示例 | 是否 shuffle |
|:---|:---|:---|:---:|
| **窄依赖** | 父 RDD 每个分区最多被子 RDD 一个分区使用 | `map` `filter` `union` | 否 |
| **宽依赖** | 父 RDD 每个分区可能被子 RDD 多个分区使用 | `groupByKey` `reduceByKey` `join` | **是** |

**为什么要区分**：Spark 遇到宽依赖就切分 Stage。窄依赖的失败恢复只需重算一个分区，宽依赖要重算上游所有分区。

## 累加器与广播变量

### 累加器 Accumulator

用于在分布式环境下做**只增不减**的计数，典型场景是统计脏数据条数。

```scala
val acc = sc.longAccumulator("errorCount")
rdd.foreach(x => if (x < 0) acc.add(1))
println(acc.value)
```

> **坑**：累加器在 **Executor 端累加，Driver 端读取**。如果在转换算子（惰性）里用累加器，不触发行动算子就不会累加，甚至可能因为任务重算而重复累加。

### 广播变量 Broadcast

把只读变量广播到每个 Executor，而不是每个 Task 复制一份。

```scala
val broadcastVar = sc.broadcast(Map(1 -> "北京", 2 -> "上海"))
rdd.map(x => broadcastVar.value.getOrElse(x, "未知"))
```

**典型场景**：大表 join 小表时，把**小表**广播出去，避免 shuffle。

## 自定义分区器

```scala
class MyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.hashCode.abs % numParts
  }
}
```

`getPartition` 的返回值必须在 `[0, numPartitions - 1]` 范围内，否则会报 `IllegalArgumentException`。

## 序列化

Spark 在 shuffle 和缓存时会把对象序列化。**自定义的类必须实现 `Serializable`**，否则报 `NotSerializableException`。

```scala
class User() extends Serializable {
  var name: String = _
}
```

**闭包检查**：算子里引用的外部变量会被序列化后发到 Executor。如果引用了 Driver 端的大型对象（如数据库连接），会直接报错——因为连接对象不可序列化。

**解决办法**：用 `@transient` 标记不需要序列化的字段，或者在 Executor 端按分区重新创建连接（用 `mapPartitions`）。
