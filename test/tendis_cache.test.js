import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert";
import { initRedis } from "./tendis.js";

const PREFIX = "cache_test::";
const redis = initRedis();

const pingResult = await redis.ping();
console.log("Redis Connection Test (PING):", pingResult);

const QUEUE_KEY = `${PREFIX}queue`;
const PRODUCER_INTERVAL = 100;
const BATCH_SIZE = 100;
const CONSUMER_INTERVAL = 10000;
const CACHE_DURATION = 11000;
const FETCH_BATCH_SIZE = 1000;
const PRODUCER_DURATION = 50000;
const CONSUMER_DURATION = 70000;

let producerTimer = null;
let consumerTimer = null;
let producerStopTimer = null;
let dataCounter = 0;
let isCleanup = false;
let isTestEnded = false;

const stats = {
  totalWritten: 0,
  totalConsumed: 0,
  totalProcessed: 0,
  totalFailed: 0,
  writeTimes: [],
  consumeTimes: []
};

/**
 * 模拟处理数据
 * @param {Array} dataList - 待处理的数据列表
 * @returns {Promise<{success: Array, failed: Array}>} - 处理成功和失败的数据
 */
async function processData(dataList) {
  const success = [];
  const failed = [];
  const successRate = 0.95;

  for (const item of dataList) {
    try {
      // await new Promise(resolve => setTimeout(resolve, 1));
      const random = Math.random();
      if (random < successRate) {
        success.push(item);
      } else {
        failed.push(item);
      }
    } catch (e) {
      failed.push(item);
    }
  }

  return { success, failed };
}

/**
 * 子程序1：模拟消息队列生产者
 * 每100ms向zset批量写入100条数据，以时间戳作为score
 */
function startProducer() {
  console.log(`[Producer] 启动生产者，每${PRODUCER_INTERVAL}ms写入${BATCH_SIZE}条数据`);

  producerTimer = setInterval(async () => {
    if (isCleanup || isTestEnded) return;

    const timestamp = Date.now();
    const members = [];

    for (let i = 0; i < BATCH_SIZE; i++) {
      const data = {
        id: dataCounter++,
        message: `test_data_${dataCounter}`,
        timestamp: timestamp + i
      };
      members.push(timestamp + i, JSON.stringify(data));
    }

    await redis.zadd(QUEUE_KEY, ...members);
    stats.totalWritten += BATCH_SIZE;

    if (dataCounter % 500 === 0) {
      const currentCount = await redis.zcard(QUEUE_KEY);
      console.log(`[Producer] 已写入: ${stats.totalWritten}条, zset当前: ${currentCount}条`);
    }
  }, PRODUCER_INTERVAL);

  producerStopTimer = setTimeout(() => {
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
 * 1. 先用 zrangebyscore 获取数据（不删除）
 * 2. 处理数据
 * 3. 处理成功后用 zrem 删除已处理的数据
 */
function startConsumer() {
  if (consumerTimer) return;
  console.log(`[Consumer] 启动消费者定时器，每${CONSUMER_INTERVAL/1000}秒执行一次`);
  consumerTimer = setInterval(consumeData, CONSUMER_INTERVAL);
}

async function consumeData() {
  if (isCleanup || isTestEnded) return;

  const executeStartTime = Date.now();
  console.log(`\n[Consumer] === 开始执行 ===`);

  clearInterval(consumerTimer);
  consumerTimer = null;

  if (isCleanup || isTestEnded) {
    console.log(`[Consumer] 检测到清理信号，跳过执行`);
    return;
  }

  const now = Date.now();
  const cutoffTime = now - CACHE_DURATION;
  const remainingBeforeProcess = await redis.zcard(QUEUE_KEY);

  console.log(`[Consumer] 当前时间: ${now}, 截止时间: ${cutoffTime}`);
  console.log(`[Consumer] zset当前: ${remainingBeforeProcess} 条`);

  let batchCount = 0;
  let batchTotalProcessed = 0;

  while (true) {
    const dataList = await redis.zrangebyscore(QUEUE_KEY, "-inf", cutoffTime, "LIMIT", 0, FETCH_BATCH_SIZE);

    if (dataList.length === 0) {
      break;
    }

    batchCount++;
    console.log(`[Consumer] 第${batchCount}批: 取出 ${dataList.length} 条数据`);

    const { success, failed } = await processData(dataList);

    if (success.length > 0) {
      const deleted = await redis.zrem(QUEUE_KEY, ...success);
      console.log(`[Consumer] 第${batchCount}批: 处理成功 ${success.length} 条, 删除成功 ${deleted} 条`);
      stats.totalProcessed += success.length;
      batchTotalProcessed += success.length;
    }

    if (failed.length > 0) {
      console.log(`[Consumer] 第${batchCount}批: 处理失败 ${failed.length} 条 (保留在zset中)`);
      stats.totalFailed += failed.length;
    }

    stats.totalConsumed += dataList.length;
  }

  stats.consumeTimes.push(now);

  const remainingAfterProcess = await redis.zcard(QUEUE_KEY);
  console.log(`[Consumer] === 执行完成, 共${batchCount}批, 处理成功${batchTotalProcessed}条, 剩余: ${remainingAfterProcess} 条 ===`);

  if (!consumerTimer && !isCleanup && !isTestEnded) {
    startConsumer();
  }
}

/**
 * 清理函数 - 按顺序执行
 * 1. 清理定时器
 * 2. 清理残留数据
 * 3. 关闭redis连接
 * 4. 退出程序
 */
async function cleanup() {
  if (isCleanup) return;
  isCleanup = true;
  isTestEnded = true;

  console.log("\n========== 开始清理资源 ==========");

  console.log("\n[步骤1/4] 清理定时器...");
  if (producerStopTimer) {
    clearTimeout(producerStopTimer);
    producerStopTimer = null;
  }
  if (producerTimer) {
    clearInterval(producerTimer);
    producerTimer = null;
    console.log("[步骤1/4] 生产者定时器已清理");
  }
  if (consumerTimer) {
    clearInterval(consumerTimer);
    consumerTimer = null;
    console.log("[步骤1/4] 消费者定时器已清理");
  }
  console.log("[步骤1/4] 所有定时器清理完成");

  console.log("\n[步骤2/4] 清理残留数据...");
  const remainingData = await redis.zcard(QUEUE_KEY);
  if (remainingData > 0) {
    await redis.del(QUEUE_KEY);
    console.log(`[步骤2/4] 已清理 ${remainingData} 条残留数据`);
  } else {
    console.log("[步骤2/4] 无残留数据");
  }

  console.log("\n[步骤3/4] 关闭Redis连接...");
  await redis.quit();
  console.log("[步骤3/4] Redis连接已关闭");

  console.log("\n[步骤4/4] 退出程序...");
  console.log("========== 清理完成 ==========\n");

  process.exit(0);
}

/**
 * 打印统计结果
 */
function printStats() {
  console.log("\n========== 测试统计结果 ==========");
  console.log(`总写入数据: ${stats.totalWritten} 条`);
  console.log(`总取出数据: ${stats.totalConsumed} 条`);
  console.log(`处理成功: ${stats.totalProcessed} 条`);
  console.log(`处理失败: ${stats.totalFailed} 条`);
  console.log(`理论写入速率: ${1000/PRODUCER_INTERVAL} 条/秒`);
  console.log(`理论消费次数: ${Math.floor(PRODUCER_DURATION/CONSUMER_INTERVAL)} 次`);
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

    console.log("\n========== 测试通过，开始清理 ==========\n");
    await cleanup();
  });
});
