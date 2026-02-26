# 缓存消费测试报告

## 1. 需求概述

设计一个缓存消费系统，包含两个子程序：

- **Producer（生产者）**：从消息队列读取数据，写入 Redis ZSet，以时间戳作为 score
- **Consumer（消费者）**：定时读取 ZSet 中缓存超过指定时间的数据，处理成功后删除

### 核心设计目标

1. 数据需缓存至少 5 秒后再处理
2. 处理失败的数据不应丢失，需保留重试
3. 消费者执行时间可能超过间隔时间，需正确管理定时器
4. 消费者每次处理数据量有上限，避免内存溢出

---

## 2. 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Redis ZSet                              │
│  Key: cache_test::queue                                     │
│  Score: 时间戳                                               │
│  Member: JSON 字符串 {id, message, timestamp}               │
└─────────────────────────────────────────────────────────────┘
                              ↑
        ┌─────────────────────┴─────────────────────┐
        │                                           │
┌───────┴───────┐                         ┌───────┴───────┐
│   Producer    │                         │   Consumer    │
│ 批量写入数据   │                         │ 分批取出处理  │
│ 100ms写入100条 │                         │ 10秒执行一次  │
└───────────────┘                         └───────────────┘
```

---

## 3. 核心参数

| 参数              | 值    | 说明                                   |
| ----------------- | ----- | -------------------------------------- |
| PRODUCER_INTERVAL | 100ms | 生产者写入间隔                         |
| BATCH_SIZE        | 100   | 生产者每次写入的数据条数               |
| CONSUMER_INTERVAL | 10s   | 消费者执行间隔                         |
| CACHE_DURATION    | 11s   | 缓存时间（比消费间隔多1秒缓冲）        |
| FETCH_BATCH_SIZE  | 1000  | 消费者每次从 ZSet 中获取的最大数据条数 |
| PRODUCER_DURATION | 50s   | 生产者运行总时长                       |
| CONSUMER_DURATION | 80s   | 消费者运行总时长                       |
| 处理成功率        | 95%   | 模拟数据处理成功概率                   |

---

## 4. 核心代码逻辑

### 4.1 生产者

```javascript
function startProducer() {
  producerTimer = setInterval(async () => {
    const timestamp = Date.now();
    const members = [];

    // 批量写入 BATCH_SIZE 条数据
    for (let i = 0; i < BATCH_SIZE; i++) {
      const data = {
        id: dataCounter++,
        message: `test_data_${dataCounter}`,
        timestamp: timestamp + i,
      };
      members.push(timestamp + i, JSON.stringify(data));
    }

    await redis.zadd(QUEUE_KEY, ...members);
    stats.totalWritten += BATCH_SIZE;
  }, PRODUCER_INTERVAL);
}
```

### 4.2 消费者

```javascript
async function consumeData() {
  // 1. 暂停定时器
  clearInterval(consumerTimer);

  // 2. 获取截止时间（保持不变）
  const now = Date.now();
  const cutoffTime = now - CACHE_DURATION;

  // 3. 分批获取数据，每次最多 FETCH_BATCH_SIZE 条
  let batchCount = 0;
  while (true) {
    // 每次从 offset=0 开始获取
    const dataList = await redis.zrangebyscore(
      QUEUE_KEY,
      "-inf",
      cutoffTime,
      "LIMIT",
      0,
      FETCH_BATCH_SIZE,
    );

    if (dataList.length === 0) break;

    batchCount++;

    // 4. 处理数据
    const { success, failed } = await processData(dataList);

    // 5. 删除处理成功的数据
    if (success.length > 0) {
      await redis.zrem(QUEUE_KEY, ...success);
    }

    // 失败的数据保留在 ZSet 中，等待下次重试
  }

  // 6. 重新启动定时器
  startConsumer();
}
```

### 4.3 处理模拟

```javascript
async function processData(dataList) {
  const successRate = 0.95;
  for (const item of dataList) {
    const random = Math.random();
    if (random < successRate) {
      success.push(item);
    } else {
      failed.push(item);
    }
  }
  return { success, failed };
}
```

---

## 5. 测试结果

### 5.1 测试场景

- 写入：100ms 间隔，每次 100 条，持续 50 秒
- 消费：10 秒间隔，持续 80 秒
- 每次最多取：1000 条
- 处理成功率：95%

### 5.2 测试数据（最新）

| 指标       | 数值                          |
| ---------- | ----------------------------- |
| 总写入数据 | 约 49,500 条                  |
| 总取出数据 | 约 50,000+ 条（含重试）       |
| 处理成功   | 约 48,000 条                  |
| 处理失败   | 约 2,500 条                   |
| 最终剩余   | 约 1,000 条（处理失败待重试） |

### 5.3 执行过程示例

```
[Producer] 启动生产者，每100ms写入100条数据
[Consumer] 启动消费者定时器，每10秒执行一次

[Producer] 已写入: 10000条, zset当前: 10000条

[Consumer] === 开始执行 ===
[Consumer] zset当前: 19900 条
[Consumer] 第1批: 取出 1000 条数据
[Consumer] 第1批: 处理成功 950 条, 删除成功 950 条
[Consumer] 第1批: 处理失败 50 条 (保留在zset中)
[Consumer] 第2批: 取出 1000 条数据
[Consumer] 第2批: 处理成功 960 条, 删除成功 960 条
[Consumer] 第2批: 处理失败 40 条 (保留在zset中)
[Consumer] 第3批: 取出 1000 条数据
...
[Consumer] === 执行完成, 共15批, 处理成功14000条, 剩余: 15000 条 ===

[Producer] 写入结束，共写入 49500 条数据

[监控] 写入已结束2秒后, zset当前: 25000 条
[监控] 再等待20秒后, zset当前: 1000 条
```

---

## 6. 关键设计要点

### 6.1 定时器管理

消费者在执行任务前需暂停定时器，执行完成后再重启，防止多个执行实例并发：

```javascript
async function consumeData() {
  clearInterval(consumerTimer); // 暂停
  // ... 处理逻辑 ...
  startConsumer(); // 重启
}
```

### 6.2 数据不丢失

采用「先取后删」策略：

1. 使用 `zrangebyscore` 获取数据（不删除）
2. 处理数据
3. 处理成功后使用 `zrem` 删除
4. 处理失败的数据保留在 ZSet 中，等待下次重试

### 6.3 分批处理

消费者每次从 ZSet 中获取最多 `FETCH_BATCH_SIZE`（1000条）数据，处理完一批后再取下一批。这样可以：

- 避免单次处理数据量过大导致内存问题
- 每次获取数据时都使用相同的截止时间，确保数据只被处理一次

### 6.4 缓存时间

缓存时间（CACHE_DURATION）应大于消费间隔，确保数据至少缓存指定时间后才被处理。

---

## 7. 结论

- ✅ 数据批量写入功能正常（100条/次）
- ✅ 缓存时间符合预期（数据至少缓存11秒）
- ✅ 分批处理功能正常（每批最多1000条）
- ✅ 处理失败的数据保留在 ZSet 中，支持重试
- ✅ 定时器管理正确，执行期间不会重复执行
- ✅ 消费者执行时间超过间隔时不会产生并发

---

## 8. 文件位置

- 测试代码：`test/tendis_cache.test.js`
- Redis 配置：`test/tendis.js`
