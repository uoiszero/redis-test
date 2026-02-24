
import { RedisIndexManager } from "../RedisIndexManager/redis_index_manager_optimized.js";
import Redis from "ioredis";

// 配置 Redis 连接
const redisConfig = {
  host: "127.0.0.1",
  port: 6379,
};

const redis = new Redis(redisConfig);
const manager = new RedisIndexManager({ redis, hashChars: 2 });

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
  console.log(`正在添加 ${data.length} 条数据...`);
  const start = Date.now();
  
  // 使用 Promise.all 并发添加，模拟真实高并发写入场景，或者分批写入
  // 为了快速准备数据，这里使用分批并发
  const BATCH_SIZE = 1000;
  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(item => manager.add(item.key, item.value)));
  }
  
  const end = Date.now();
  console.log(`数据添加完成，耗时: ${(end - start) / 1000}s`);
}

/**
 * 测试单个删除 (del)
 * @param {Array<{key: string, value: string}>} data 
 */
async function testDel(data) {
  console.log(`开始测试逐条删除 (del) ${data.length} 条数据...`);
  const start = Date.now();
  
  for (const item of data) {
    await manager.del(item.key);
  }
  
  const end = Date.now();
  const duration = (end - start) / 1000;
  console.log(`逐条删除完成，耗时: ${duration}s, TPS: ${(data.length / duration).toFixed(2)}`);
  return duration;
}

/**
 * 测试批量删除 (mDel)
 * @param {Array<{key: string, value: string}>} data 
 */
async function testMDel(data) {
  console.log(`开始测试批量删除 (mDel) ${data.length} 条数据...`);
  const keys = data.map(item => item.key);
  const start = Date.now();
  
  await manager.mDel(keys);
  
  const end = Date.now();
  const duration = (end - start) / 1000;
  console.log(`批量删除完成，耗时: ${duration}s, TPS: ${(data.length / duration).toFixed(2)}`);
  return duration;
}

async function runBenchmark(count) {
  console.log(`\n========== 开始 ${count} 条数据基准测试 ==========`);
  const data = generateData(count);

  // 1. 准备数据
  await batchAdd(data);

  // 2. 测试逐条删除
  const delTime = await testDel(data);

  // 3. 重新准备数据
  await batchAdd(data);

  // 4. 测试批量删除
  const mDelTime = await testMDel(data);

  console.log(`\n========== ${count} 条数据测试结果 ==========`);
  console.log(`逐条删除 (del): ${delTime}s`);
  console.log(`批量删除 (mDel): ${mDelTime}s`);
  console.log(`性能提升: ${(delTime / mDelTime).toFixed(2)}x`);
}

async function main() {
  try {
    // 确保连接
    await manager._ensureConnection();

    // 清理可能的旧数据
    console.log("清理旧数据...");
    const keys = await redis.keys("user:benchmark:*");
    if (keys.length > 0) {
        await redis.del(keys);
    }
    // 清理索引 (简单处理，直接清空相关 Pattern 的 Key，生产环境慎用)
    // 注意：这里为了测试方便，我们假设测试库是干净的或者只清理 benchmark 相关
    // 由于索引分散在 bucket 中，彻底清理比较麻烦，这里我们依赖 del/mDel 的正确性
    // 或者我们可以先手动清理一下 idx:* 
    const indexKeys = await redis.keys("idx:*");
    if (indexKeys.length > 0) {
        await redis.del(indexKeys);
    }

    // 测试 1万条
    await runBenchmark(10000);

    // 测试 10万条
    await runBenchmark(100000);

  } catch (err) {
    console.error("测试出错:", err);
  } finally {
    redis.disconnect();
  }
}

main();
