
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

    // 1. 写入一些数据以产生统计
    console.log("Writing test data...");
    const WRITE_COUNT = 5000;
    const batchPromises = [];
    for (let i = 0; i < WRITE_COUNT; i++) {
        const id = i.toString().padStart(5, "0");
        const key = `debug:user:${id}`;
        batchPromises.push(manager.add(key, "val"));
    }
    await Promise.all(batchPromises);

    // 2. 调用 getDebugStats
    console.log("\nGetting debug stats...");
    const stats = await manager.getDebugStats();
    
    console.log("=== Debug Stats (Summary) ===");
    console.log(JSON.stringify(stats, null, 2));

    // 3. 验证数据合理性
    if (stats.stats.totalItems !== WRITE_COUNT) {
        throw new Error(`Total items mismatch! Expected ${WRITE_COUNT}, got ${stats.stats.totalItems}`);
    }

    if (stats.meta.totalBuckets !== 256) {
        throw new Error(`Total buckets mismatch! Expected 256, got ${stats.meta.totalBuckets}`);
    }
    
    // 检查是否有严重倾斜 (例如最大桶比平均值大太多)
    const skewRatio = stats.stats.maxItems / stats.stats.avgItems;
    console.log(`\nSkew Ratio (Max/Avg): ${skewRatio.toFixed(2)}`);
    if (skewRatio > 3) {
        console.warn("Warning: High data skew detected!");
    } else {
        console.log("Data distribution looks healthy.");
    }

    // 4. 清理数据
    console.log("\nCleaning up...");
    const keys = await redis.keys("debug:user:*");
    if (keys.length > 0) {
        await manager.del(keys);
    }
    
    // 5. 再次验证 (应该是空的)
    const emptyStats = await manager.getDebugStats();
    if (emptyStats.stats.totalItems !== 0) {
        console.warn("Cleanup might be incomplete.");
    } else {
        console.log("Cleanup verified.");
    }

  } catch (err) {
    console.error("Test failed:", err);
  } finally {
    redis.disconnect();
  }
}

main();
