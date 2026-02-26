# 缓存消费测试报告

## 1. 需求概述

设计一个缓存消费系统，包含两个子程序：

- **Producer（生产者）**：从消息队列读取数据，写入 Redis ZSet，以时间戳作为 score
- **Consumer（消费者）**：定时读取 ZSet 中缓存超过指定时间的数据，处理成功后删除

### 核心设计目标

1. 数据需缓存至少 5 秒后再处理
2. 处理失败的数据不应丢失，需保留重试
3. 消费者执行时间可能超过间隔时间，需正确管理定时器

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
│  写入数据      │                         │  取出并处理   │
│ 100ms/次      │                         │  10秒/次       │
└───────────────┘                         └───────────────┘
```

---

## 3. 核心参数

| 参数 | 值 | 说明 |
|------|-----|------|
| PRODUCER_INTERVAL | 100ms | 生产者写入间隔 |
| CONSUMER_INTERVAL | 10s | 消费者执行间隔 |
| CACHE_DURATION | 11s | 缓存时间（比消费间隔多1秒缓冲） |
| PRODUCER_DURATION | 50s | 生产者运行总时长 |
| CONSUMER_DURATION | 70s | 消费者运行总时长 |
| 处理成功率 | 95% | 模拟数据处理成功概率 |

---

## 4. 核心代码逻辑

### 4.1 生产者

```javascript
function startProducer() {
  producerTimer = setInterval(async () => {
    const timestamp = Date.now();
    const data = { id: dataCounter++, timestamp };
    await redis.zadd(QUEUE_KEY, timestamp, JSON.stringify(data));
  }, PRODUCER_INTERVAL);
}
```

### 4.2 消费者

```javascript
async function consumeData() {
  // 1. 暂停定时器
  clearInterval(consumerTimer);

  // 2. 获取截止时间
  const cutoffTime = Date.now() - CACHE_DURATION;

  // 3. 取出数据（不删除）
  const dataList = await redis.zrangebyscore(QUEUE_KEY, "-inf", cutoffTime);

  // 4. 处理数据
  const { success, failed } = await processData(dataList);

  // 5. 删除处理成功的数据
  if (success.length > 0) {
    await redis.zrem(QUEUE_KEY, ...success);
  }

  // 失败的数据保留在 ZSet 中，等待下次重试

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

- 写入：100ms 间隔，持续 50 秒
- 消费：10 秒间隔，持续 70 秒
- 处理成功率：95%

### 5.2 测试数据

| 指标 | 数值 |
|------|------|
| 总写入数据 | 495 条 |
| 总取出数据 | 510 条 |
| 处理成功 | 490 条 |
| 处理失败 | 20 条 |
| 最终剩余 | 5 条 |

### 5.3 执行过程示例

```
[Producer] 写入数据, zset当前: 50条
[Consumer] 取出 0 条, 剩余: 99 条 (数据未满11秒缓存)
[Producer] 写入数据, zset当前: 100条
[Consumer] 取出 90 条, 成功 89 条, 失败 1 条, 剩余: 113 条
[Producer] 写入数据, zset当前: 161条
[Consumer] 取出 104 条, 成功 99 条, 失败 5 条, 剩余: 117 条
[Producer] 写入结束, 共写入 495 条
[Consumer] 取出 101 条, 成功 96 条, 失败 5 条, 剩余: 5 条 (最终)
```

---

## 6. 关键设计要点

### 6.1 定时器管理

消费者在执行任务前需暂停定时器，执行完成后再重启，防止多个执行实例并发：

```javascript
async function consumeData() {
  clearInterval(consumerTimer);  // 暂停
  // ... 处理逻辑 ...
  startConsumer();              // 重启
}
```

### 6.2 数据不丢失

采用「先取后删」策略：

1. 使用 `zrangebyscore` 获取数据（不删除）
2. 处理数据
3. 处理成功后使用 `zrem` 删除
4. 处理失败的数据保留在 ZSet 中，等待下次重试

### 6.3 缓存时间

缓存时间（CACHE_DURATION）应大于消费间隔，确保数据至少缓存指定时间后才被处理。

---

## 7. 结论

- ✅ 数据写入和消费功能正常
- ✅ 缓存时间符合预期（数据至少缓存11秒）
- ✅ 处理失败的数据保留在 ZSet 中，支持重试
- ✅ 定时器管理正确，执行期间不会重复执行
- ✅ 最终剩余数据为处理失败的数据，符合预期

---

## 8. 文件位置

- 测试代码：`test/tendis_cache.test.js`
- Redis 配置：`test/tendis.js`
