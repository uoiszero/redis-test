import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert";
import { initRedis } from "./tendis.js";

const PREFIX = "cache_test::";
const redis = initRedis();

const pingResult = await redis.ping();
console.log("Redis Connection Test (PING):", pingResult);

const QUEUE_KEY = `${PREFIX}queue`;
const PRODUCER_INTERVAL = 100;
const CONSUMER_INTERVAL = 10000;
const CACHE_DURATION = 11000;
const PRODUCER_DURATION = 50000;
const CONSUMER_DURATION = 70000;

let producerTimer = null;
let consumerTimer = null;
let dataCounter = 0;
let isCleanup = false;

const stats = {
  totalWritten: 0,
  totalConsumed: 0,
  writeTimes: [],
  consumeTimes: []
};

/**
 * 子程序1：模拟消息队列生产者
 * 每100ms向zset写入一条数据，以时间戳作为score
 */
function startProducer() {
  console.log(`[Producer] 启动生产者，每${PRODUCER_INTERVAL}ms写入数据`);
  const startTime = Date.now();

  producerTimer = setInterval(async () => {
    const timestamp = Date.now();
    const data = {
      id: dataCounter++,
      message: `test_data_${dataCounter}`,
      timestamp: timestamp
    };
    await redis.zadd(QUEUE_KEY, timestamp, JSON.stringify(data));
    stats.totalWritten++;
    stats.writeTimes.push(timestamp - startTime);

    if (dataCounter % 50 === 0) {
      const currentCount = await redis.zcard(QUEUE_KEY);
      console.log(`[Producer] 已写入: ${stats.totalWritten}条, zset当前: ${currentCount}条`);
    }
  }, PRODUCER_INTERVAL);

  setTimeout(() => {
    if (producerTimer) {
      clearInterval(producerTimer);
      producerTimer = null;
      console.log(`[Producer] 写入结束，共写入 ${stats.totalWritten} 条数据`);
    }
  }, PRODUCER_DURATION);
}

/**
 * 子程序2：缓存消费者
 * 每10秒执行一次，执行前暂停定时器，执行完后重新启动
 * 使用zremrangebyscore取出并删除缓存时间之前的数据
 */
function startConsumer() {
  console.log(`[Consumer] 启动消费者，每${CONSUMER_INTERVAL/1000}秒执行一次`);
  consumerTimer = setInterval(consumeData, CONSUMER_INTERVAL);
}

async function consumeData() {
  const executeStartTime = Date.now();
  console.log(`\n[Consumer] === 开始执行 ===`);

  clearInterval(consumerTimer);
  consumerTimer = null;

  const now = Date.now();
  const cutoffTime = now - CACHE_DURATION;

  const consumed = await redis.zremrangebyscore(QUEUE_KEY, "-inf", cutoffTime);
  const remaining = await redis.zcard(QUEUE_KEY);

  stats.totalConsumed += consumed;
  stats.consumeTimes.push(now);

  console.log(`[Consumer] 当前时间: ${now}, 截止时间: ${cutoffTime}`);
  console.log(`[Consumer] 取出并删除了 ${consumed} 条数据, 剩余: ${remaining} 条`);
  console.log(`[Consumer] === 执行完成 ===`);

  if (!consumerTimer) {
    startConsumer();
  }
}

/**
 * 清理函数，停止所有定时器
 */
async function cleanup() {
  if (isCleanup) return;
  isCleanup = true;

  console.log("\n========== 清理资源 ==========");
  if (producerTimer) {
    clearInterval(producerTimer);
    producerTimer = null;
  }
  if (consumerTimer) {
    clearInterval(consumerTimer);
    consumerTimer = null;
  }
  await redis.del(QUEUE_KEY);
  await redis.quit();
}

/**
 * 打印统计结果
 */
function printStats() {
  console.log("\n========== 测试统计结果 ==========");
  console.log(`总写入数据: ${stats.totalWritten} 条`);
  console.log(`总消费数据: ${stats.totalConsumed} 条`);
  console.log(`理论写入速率: ${1000/PRODUCER_INTERVAL} 条/秒`);
  console.log(`理论消费次数: ${Math.floor(PRODUCER_DURATION/CONSUMER_INTERVAL)} 次`);
  console.log(`预期消费数据: ~${Math.floor(PRODUCER_DURATION/1000 * 1000/PRODUCER_INTERVAL * (CACHE_DURATION/1000 - 1))} 条`);
  console.log("==================================\n");
}

describe("缓存消费测试 - 新参数", () => {
  test("100ms写入/10秒消费模式测试", async () => {
    console.log("\n========== 测试开始 ==========");
    console.log(`写入间隔: ${PRODUCER_INTERVAL}ms`);
    console.log(`消费间隔: ${CONSUMER_INTERVAL/1000}秒`);
    console.log(`缓存时间: ${CACHE_DURATION/1000}秒`);
    console.log(`写入时长: ${PRODUCER_DURATION/1000}秒`);
    console.log(`消费时长: ${CONSUMER_DURATION/1000}秒`);
    console.log("================================\n");

    startProducer();
    startConsumer();

    await new Promise(resolve => setTimeout(resolve, PRODUCER_DURATION + 2000));
    console.log(`\n[监控] 写入已结束2秒后, zset当前: ${await redis.zcard(QUEUE_KEY)} 条`);

    await new Promise(resolve => setTimeout(resolve, 20000));
    console.log(`\n[监控] 再等待20秒后, zset当前: ${await redis.zcard(QUEUE_KEY)} 条`);

    printStats();

    console.log("\n========== 测试通过，清理中 ==========\n");
    await cleanup();
  });

  after(async () => {
    if (!isCleanup) {
      await cleanup();
    }
  });
});
