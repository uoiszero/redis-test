import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert";
import { initRedis } from "./tendis.js";

const PREFIX = "cache_test::";
const redis = initRedis();

const pingResult = await redis.ping();
console.log("Redis Connection Test (PING):", pingResult);

const QUEUE_KEY = `${PREFIX}queue`;
const PRODUCER_INTERVAL = 500;
const CONSUMER_INTERVAL = 5000;
const CACHE_DURATION = 5500;

let producerTimer = null;
let consumerTimer = null;
let dataCounter = 0;
let isCleanup = false;

/**
 * 子程序1：模拟消息队列生产者
 * 每500ms向zset写入一条数据，以时间戳作为score
 */
function startProducer() {
  console.log("[Producer] 启动生产者，每500ms写入数据");
  producerTimer = setInterval(async () => {
    const timestamp = Date.now();
    const data = {
      id: dataCounter++,
      message: `test_data_${dataCounter}`,
      timestamp: timestamp
    };
    await redis.zadd(QUEUE_KEY, timestamp, JSON.stringify(data));
    console.log(`[Producer] 写入数据: id=${data.id}, score=${timestamp}`);
  }, PRODUCER_INTERVAL);
}

/**
 * 子程序2：缓存消费者
 * 每5秒执行一次，执行前暂停定时器，执行完后重新启动
 * 使用zremrangebyscore取出并删除5秒前的数据
 */
function startConsumer() {
  console.log("[Consumer] 启动消费者，每5秒执行一次");
  consumerTimer = setInterval(consumeData, CONSUMER_INTERVAL);
}

async function consumeData() {
  console.log(`[Consumer] === 开始执行 ===`);

  clearInterval(consumerTimer);
  consumerTimer = null;

  const now = Date.now();
  const cutoffTime = now - CACHE_DURATION;

  const results = await redis.zremrangebyscore(QUEUE_KEY, "-inf", cutoffTime);

  console.log(`[Consumer] 当前时间: ${now}, 截止时间: ${cutoffTime}`);
  console.log(`[Consumer] 取出并删除了 ${results} 条数据`);

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

  console.log("\n[Cleanup] 清理资源...");
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
  console.log("[Cleanup] 清理完成，程序退出");
}

/**
 * 等待指定时间后清理
 * @param {number} ms - 等待毫秒数
 */
function waitAndCleanup(ms) {
  setTimeout(() => {
    cleanup().then(() => {
      process.exit(0);
    });
  }, ms);
}

describe("缓存消费测试", () => {
  test("缓存数据5秒后处理 - 生产者消费者模式", async () => {
    console.log("\n========== 测试开始 ==========\n");

    startProducer();

    await new Promise(resolve => setTimeout(resolve, 1000));
    const count1 = await redis.zcard(QUEUE_KEY);
    console.log(`[Test] 1秒后zset中数据量: ${count1}`);

    await new Promise(resolve => setTimeout(resolve, 5000));
    const count2 = await redis.zcard(QUEUE_KEY);
    console.log(`[Test] 6秒后zset中数据量: ${count2}`);

    await new Promise(resolve => setTimeout(resolve, 5000));
    const count3 = await redis.zcard(QUEUE_KEY);
    console.log(`[Test] 11秒后zset中数据量: ${count3}`);

    assert.ok(count1 > 0, "1秒后应该有数据写入");
    console.log("\n========== 测试通过，10秒后自动清理 ==========\n");

    waitAndCleanup(10000);
  });

  after(async () => {
    await cleanup();
  });
});
