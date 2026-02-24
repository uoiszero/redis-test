
import { RedisIndexManager } from "../RedisIndexManager/redis_index_manager_optimized.js";
import Redis from "ioredis";

// 配置 Redis 连接
const redisConfig = {
  host: "127.0.0.1",
  port: 6379,
};

const redis = new Redis(redisConfig);
const manager = new RedisIndexManager({ redis, hashChars: 2 });

const SMALL_TESTS = [1, 10, 100, 1000];
const LARGE_TESTS = [10000, 100000];

// 前4个数据量跑100次取平均值
const SMALL_ITERATIONS = 100;
// 后2个数据量跑5次取平均值，防止时间过长
const LARGE_ITERATIONS = 5;

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
  const BATCH_SIZE = 1000;
  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(item => manager.add(item.key, item.value)));
  }
}

/**
 * 测试新的 del 方法
 * @param {Array<{key: string, value: string}>} data 
 * @returns {Promise<number>} 耗时 (ms)
 */
async function runDelTest(data) {
  const keys = data.map(item => item.key);
  const start = performance.now();
  
  // 对于单条数据，我们可以测试传递字符串，也可以传递数组
  // 为了与之前的 mDel 逻辑保持一致，如果是 >1 条，传数组
  // 如果是 1 条，传数组或字符串都可以，这里统一传数组以测试批量逻辑的性能
  // 实际上新的 del 内部会自动把 string 转为 array
  
  if (keys.length === 1) {
    await manager.del(keys[0]); // 测试单个字符串传参 (兼容性)
  } else {
    await manager.del(keys);
  }
  
  const end = performance.now();
  return end - start;
}

async function runBenchmark(count, iterations) {
  console.log(`\n========== 开始测试 ${count} 条数据 (重复 ${iterations} 次) ==========`);
  
  const data = generateData(count);
  let totalTime = 0;

  // 预热/清理
  await batchAdd(data);
  await manager.del(data.map(d => d.key));

  for (let i = 0; i < iterations; i++) {
    // 1. 准备数据
    await batchAdd(data);
    
    // 2. 测试 del
    totalTime += await runDelTest(data);
    
    // 显示进度
    if (iterations > 10 && (i + 1) % 10 === 0) {
        process.stdout.write(`.`);
    }
  }
  if (iterations > 10) console.log(""); // 换行

  const avgTime = totalTime / iterations;
  const tps = (count * 1000) / avgTime;

  console.log(`平均耗时: ${avgTime.toFixed(4)} ms`);
  console.log(`TPS: ${tps.toFixed(2)}`);
  
  return avgTime;
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
    
    console.log("=== 新版 del 方法基准测试 ===");

    // 小数据量测试 (100次平均)
    for (const count of SMALL_TESTS) {
      await runBenchmark(count, SMALL_ITERATIONS);
    }

    // 大数据量测试 (5次平均)
    for (const count of LARGE_TESTS) {
      await runBenchmark(count, LARGE_ITERATIONS);
    }

  } catch (err) {
    console.error("测试出错:", err);
  } finally {
    redis.disconnect();
  }
}

main();
