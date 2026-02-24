
import { RedisIndexManager } from "../RedisIndexManager/redis_index_manager_optimized.js";
import Redis from "ioredis";

// 配置 Redis 连接
const redisConfig = {
  host: "127.0.0.1",
  port: 6379,
};

const redis = new Redis(redisConfig);
const manager = new RedisIndexManager({ redis, hashChars: 2 });

async function main() {
  try {
    await manager._ensureConnection();

    console.log("=== 测试新的 del 方法 ===");

    // 1. 测试删除单个字符串
    const key1 = "user:del:test:1";
    await manager.add(key1, "value1");
    console.log(`Added ${key1}`);
    
    // 验证存在
    let count = await manager.count("user:del:test:1", "user:del:test:1");
    if (count !== 1) throw new Error(`${key1} 应该存在`);

    console.log(`Deleting ${key1} (string)...`);
    await manager.del(key1);
    
    // 验证删除
    count = await manager.count("user:del:test:1", "user:del:test:1");
    if (count !== 0) throw new Error(`${key1} 应该被删除`);
    console.log(`Deleted ${key1} successfully.`);


    // 2. 测试删除字符串数组
    const key2 = "user:del:test:2";
    const key3 = "user:del:test:3";
    await manager.add(key2, "value2");
    await manager.add(key3, "value3");
    console.log(`Added ${key2}, ${key3}`);

    console.log(`Deleting [${key2}, ${key3}] (array)...`);
    await manager.del([key2, key3]);

    // 验证删除
    count = await manager.count("user:del:test:2", "user:del:test:3");
    if (count !== 0) throw new Error(`${key2} 和 ${key3} 应该被删除`);
    console.log(`Deleted array successfully.`);

    console.log("\n所有测试通过！");

  } catch (err) {
    console.error("测试失败:", err);
  } finally {
    redis.disconnect();
  }
}

main();
