import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert";
import Redis from "ioredis";

const PREFIX = "sort_set_test::";

const singletonConfig = {
  host: "localhost",
  port: 6379,
};

const clusterConfig = [
  {
    host: "192.168.5.9",
    port: 51002,
  },
  {
    host: "192.168.5.10",
    port: 51002,
  },
  {
    host: "192.168.5.11",
    port: 51002,
  },
];

const mode = process.env.TEST_MODE || "cluster";
console.log(`Running test in ${mode} mode...`);
const config = mode === "cluster" ? clusterConfig : singletonConfig;

/**
 *
 * @returns 初始化redis客户端
 */
function initRedis() {
  const redisOptions = {
    connectTimeout: 5000, // 5s timeout
    maxRetriesPerRequest: 1,
    enableReadyCheck: false,
    retryStrategy: times => {
      if (times > 3) return null; // Stop retrying after 3 attempts
      return Math.min(times * 100, 2000);
    },
  };

  if (Array.isArray(config)) {
    console.log("Config is an array, initializing Redis Cluster...");
    return new Redis.Cluster(config, {
      redisOptions: redisOptions,
      clusterRetryStrategy: times => {
        if (times > 3) return null;
        return Math.min(times * 100, 2000);
      },
    });
  } else {
    console.log("Config is an object, initializing Redis Standalone...");
    return new Redis({ ...config, ...redisOptions });
  }
}

const redis = initRedis();

/**
 * 测试redis连接是否成功
 */
const pingResult = await redis.ping();
console.log("Redis Connection Test (PING):", pingResult);

const TEST_KEY = `${PREFIX}benchmark`;
const DATA_COUNT = 100000;
const FETCH_BATCH_SIZE = 1000;
const FETCH_TOTAL = 5000;
const BATCH_ADD_SIZE = 5000;
const BATCH_DELETE_SIZE = 5000;

/**
 * 分批添加数据到有序集合，解决大数组栈溢出问题
 * @param {object} redisClient - Redis客户端
 * @param {string} key - 有序集合键名
 * @param {number} count - 总数据量
 * @returns {Promise<number>} - 添加的总数量
 */
async function batchZadd(redisClient, key, count) {
  let totalAdded = 0;
  for (let i = 0; i < count; i += BATCH_ADD_SIZE) {
    const members = [];
    const end = Math.min(i + BATCH_ADD_SIZE, count);
    for (let j = i; j < end; j++) {
      members.push(j, `member_${j}`);
    }
    const added = await redisClient.zadd(key, ...members);
    totalAdded += added;
  }
  return totalAdded;
}

/**
 * 分批删除有序集合中的数据
 * @param {object} redisClient - Redis客户端
 * @param {string} key - 有序集合键名
 * @param {number} count - 总数据量
 * @returns {Promise<void>}
 */
async function batchZrem(redisClient, key, count) {
  for (let i = 0; i < count; i += BATCH_DELETE_SIZE) {
    const end = Math.min(i + BATCH_DELETE_SIZE, count);
    const members = [];
    for (let j = i; j < end; j++) {
      members.push(`member_${j}`);
    }
    await redisClient.zrem(key, ...members);
  }
}

describe("ZSet 性能测试", () => {
  afterEach(async () => {
    await redis.del(TEST_KEY);
  });

  after(async () => {
    await redis.quit();
  });

  test("添加10000条数据，以序号作为排序依据", async () => {
    const startTime = Date.now();
    await batchZadd(redis, TEST_KEY, DATA_COUNT);
    const addTime = Date.now() - startTime;

    const count = await redis.zcard(TEST_KEY);
    assert.strictEqual(
      count,
      DATA_COUNT,
      `期望添加 ${DATA_COUNT} 条数据，实际添加 ${count} 条`,
    );

    const firstMember = await redis.zrange(TEST_KEY, 0, 0);
    assert.strictEqual(firstMember[0], "member_0", "第一条数据应该是 member_0");

    console.log(`添加 ${DATA_COUNT} 条数据耗时: ${addTime}ms`);
  });

  test("分批取出前500条，每批100条", async () => {
    await batchZadd(redis, TEST_KEY, DATA_COUNT);

    let totalFetchTime = 0;
    let batchCount = FETCH_TOTAL / FETCH_BATCH_SIZE;

    for (let batch = 0; batch < batchCount; batch++) {
      const startIndex = batch * FETCH_BATCH_SIZE;
      const endIndex = startIndex + FETCH_BATCH_SIZE - 1;

      const startTime = Date.now();
      const results = await redis.zrange(TEST_KEY, startIndex, endIndex);
      const fetchTime = Date.now() - startTime;

      totalFetchTime += fetchTime;

      assert.strictEqual(
        results.length,
        FETCH_BATCH_SIZE,
        `第 ${batch + 1} 批应该返回 ${FETCH_BATCH_SIZE} 条，实际返回 ${results.length} 条`,
      );

      assert.strictEqual(
        results[0],
        `member_${startIndex}`,
        `第 ${batch + 1} 批第一条应该是 member_${startIndex}`,
      );

      console.log(
        `第 ${batch + 1} 批 (索引 ${startIndex}-${endIndex}) 耗时: ${fetchTime}ms, 返回 ${results.length} 条`,
      );
    }

    console.log(`分批取出 ${FETCH_TOTAL} 条数据总耗时: ${totalFetchTime}ms`);
    console.log(`平均每批耗时: ${totalFetchTime / batchCount}ms`);
  });

  test("删除集合中的所有数据", async () => {
    await batchZadd(redis, TEST_KEY, DATA_COUNT);

    const countBefore = await redis.zcard(TEST_KEY);
    assert.strictEqual(countBefore, DATA_COUNT, "删除前应该有数据");

    const startTime = Date.now();
    const deletedCount = await redis.del(TEST_KEY);
    const deleteTime = Date.now() - startTime;

    assert.strictEqual(deletedCount, 1, "应该删除1个key");

    const countAfter = await redis.zcard(TEST_KEY);
    assert.strictEqual(countAfter, 0, "删除后集合应该为空");

    console.log(`删除 ${DATA_COUNT} 条数据耗时: ${deleteTime}ms`);
  });

  test("分批删除集合中的数据", async () => {
    await batchZadd(redis, TEST_KEY, DATA_COUNT);

    const countBefore = await redis.zcard(TEST_KEY);
    assert.strictEqual(countBefore, DATA_COUNT, "删除前应该有数据");

    const startTime = Date.now();
    await batchZrem(redis, TEST_KEY, DATA_COUNT);
    const deleteTime = Date.now() - startTime;

    const countAfter = await redis.zcard(TEST_KEY);
    assert.strictEqual(countAfter, 0, "删除后集合应该为空");

    console.log(`分批删除 ${DATA_COUNT} 条数据耗时: ${deleteTime}ms`);
  });

  test("完整流程：添加->取出->删除", async () => {
    const startTime = Date.now();

    const addStartTime = Date.now();
    await batchZadd(redis, TEST_KEY, DATA_COUNT);
    const addTime = Date.now() - addStartTime;

    let fetchStartTime = Date.now();
    for (let batch = 0; batch < FETCH_TOTAL / FETCH_BATCH_SIZE; batch++) {
      const startIndex = batch * FETCH_BATCH_SIZE;
      const endIndex = startIndex + FETCH_BATCH_SIZE - 1;
      await redis.zrange(TEST_KEY, startIndex, endIndex);
    }
    const fetchTime = Date.now() - fetchStartTime;

    const deleteStartTime = Date.now();
    await redis.del(TEST_KEY);
    const deleteTime = Date.now() - deleteStartTime;

    const totalTime = Date.now() - startTime;

    console.log(`完整流程测试:`);
    console.log(`  添加 ${DATA_COUNT} 条数据: ${addTime}ms`);
    console.log(`  分批取出 ${FETCH_TOTAL} 条数据: ${fetchTime}ms`);
    console.log(`  删除集合: ${deleteTime}ms`);
    console.log(`  总耗时: ${totalTime}ms`);
  });
});
