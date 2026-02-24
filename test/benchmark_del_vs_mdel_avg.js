
import { RedisIndexManager } from "../RedisIndexManager/redis_index_manager_optimized.js";
import Redis from "ioredis";

// 配置 Redis 连接
const redisConfig = {
  host: "127.0.0.1",
  port: 6379,
};

const redis = new Redis(redisConfig);
const manager = new RedisIndexManager({ redis, hashChars: 2 });

const TEST_COUNTS = [1, 10, 100, 1000];
const ITERATIONS = 100;

/**
 * 生成测试数据
 * @param {number} count 
 * @returns {Array<{key: string, value: string}>}
 */
function generateData(count) {
  const data = [];
  for (let i = 0; i < count; i++) {
    data.push({
      key: `user:benchmark:${i}`,
      value: JSON.stringify({ name: `User ${i}`, age: 20 + (i % 50) }),
    });
  }
  return data;
}

/**
 * 批量添加数据
 * @param {Array<{key: string, value: string}>} data 
 */
async function batchAdd(data) {
  // 简单起见，全部并发
  await Promise.all(data.map(item => manager.add(item.key, item.value)));
}

/**
 * 测试单个删除 (del)
 * @param {Array<{key: string, value: string}>} data 
 * @returns {Promise<number>} 耗时 (ms)
 */
async function runDelTest(data) {
  const start = performance.now();
  for (const item of data) {
    await manager.del(item.key);
  }
  const end = performance.now();
  return end - start;
}

/**
 * 测试批量删除 (mDel)
 * @param {Array<{key: string, value: string}>} data 
 * @returns {Promise<number>} 耗时 (ms)
 */
async function runMDelTest(data) {
  const keys = data.map(item => item.key);
  const start = performance.now();
  await manager.mDel(keys);
  const end = performance.now();
  return end - start;
}

async function runBenchmarkForCount(count) {
  console.log(`\n========== 开始测试 ${count} 条数据 (重复 ${ITERATIONS} 次) ==========`);
  
  const data = generateData(count);
  let totalDelTime = 0;
  let totalMDelTime = 0;

  // 预热/清理
  await batchAdd(data);
  await manager.mDel(data.map(d => d.key));

  for (let i = 0; i < ITERATIONS; i++) {
    // 1. 准备数据 for del
    await batchAdd(data);
    // 2. 测试 del
    totalDelTime += await runDelTest(data);

    // 3. 准备数据 for mDel
    await batchAdd(data);
    // 4. 测试 mDel
    totalMDelTime += await runMDelTest(data);
    
    // 显示进度
    if ((i + 1) % 20 === 0) {
        process.stdout.write(`.`);
    }
  }
  console.log(""); // 换行

  const avgDelTime = totalDelTime / ITERATIONS;
  const avgMDelTime = totalMDelTime / ITERATIONS;
  const tpsDel = (count * 1000) / avgDelTime;
  const tpsMDel = (count * 1000) / avgMDelTime;

  console.log(`平均耗时 (ms):`);
  console.log(`  逐条删除 (del): ${avgDelTime.toFixed(4)} ms`);
  console.log(`  批量删除 (mDel): ${avgMDelTime.toFixed(4)} ms`);
  console.log(`TPS (操作/秒):`);
  console.log(`  逐条删除 (del): ${tpsDel.toFixed(2)}`);
  console.log(`  批量删除 (mDel): ${tpsMDel.toFixed(2)}`);
  
  if (avgMDelTime > 0) {
    console.log(`性能提升: ${(avgDelTime / avgMDelTime).toFixed(2)}x`);
  } else {
    console.log(`性能提升: N/A (耗时极短)`);
  }
}

async function main() {
  try {
    // 确保连接
    await manager._ensureConnection();

    // 清理可能的旧数据
    const keys = await redis.keys("user:benchmark:*");
    if (keys.length > 0) {
        await redis.del(keys);
    }
    
    // 执行测试
    for (const count of TEST_COUNTS) {
      await runBenchmarkForCount(count);
    }

  } catch (err) {
    console.error("测试出错:", err);
  } finally {
    redis.disconnect();
  }
}

main();
