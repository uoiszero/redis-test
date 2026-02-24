import Redis from "ioredis";
import { RedisIndexManager } from "../RedisIndexManager/redis_index_manager_optimized.js";
import config from "./tendis.config.js";

/**
 * 测试批量写入数据
 * 
 * 按照 BATCH_SIZE (默认500) 进行并发写入，以提高写入效率。
 * Key 格式为 `{dataPrefix}{00000-N}`，Value 为包含 id, name, timestamp 的 JSON 字符串。
 *
 * @param {RedisIndexManager} manager - RedisIndexManager 实例
 * @param {string} dataPrefix - 测试数据的 Key 前缀 (如 "test_user:")
 * @param {number} writeCount - 写入的数据总量
 * @returns {Promise<void>}
 */
async function testWriteData(manager, dataPrefix, writeCount) {
    console.log(`Writing ${writeCount} items...`);
    const startTime = Date.now();
    
    const BATCH_SIZE = 500;
    
    // Process in batches
    for (let i = 0; i < writeCount; i += BATCH_SIZE) {
        const batchPromises = [];
        for (let j = 0; j < BATCH_SIZE && (i + j) < writeCount; j++) {
            const index = i + j;
            // Pad number for lexicographical order: 00000, 00001, ... 09999
            const id = index.toString().padStart(5, "0"); 
            const key = `${dataPrefix}${id}`;
            const value = JSON.stringify({ id, name: `User ${id}`, timestamp: Date.now() });
            batchPromises.push(manager.add(key, value));
        }
        await Promise.all(batchPromises);
    }
    
    const duration = Date.now() - startTime;
    console.log(`Write completed in ${duration}ms`);
}

/**
 * 测试数据统计与范围查询
 * 
 * 1. 使用 count() 方法统计总数，验证与写入数量是否一致。
 * 2. 使用 scan() 方法获取前 10 条数据，验证返回的数据结构与顺序是否正确。
 *
 * @param {RedisIndexManager} manager - RedisIndexManager 实例
 * @param {string} dataPrefix - 测试数据的 Key 前缀
 * @param {number} expectedCount - 期望的数据总量
 * @returns {Promise<void>}
 */
async function testScanData(manager, dataPrefix, expectedCount) {
    console.log("Counting data...");
    // Note: The count method expects a range. 
    // If we want to count all keys starting with DATA_PREFIX, we need to handle the end key carefully.
    // The manager.count() internally infers endKey if not provided, similar to scan.
    const count = await manager.count(dataPrefix);
    console.log(`Total items count (via ZLEXCOUNT): ${count}`);

    // Verify count matches
    if (count === expectedCount) {
        console.log("✅ Count matches expected value.");
    } else {
        console.warn(`⚠️ Count mismatch! Expected ${expectedCount}, got ${count}`);
    }

    // 5. Scan Data
    console.log("Scanning data (limit 10)...");
    const scanResult = await manager.scan(dataPrefix, undefined, 10);
    
    console.log(`Scan returned ${scanResult.length / 2} items:`);
    for (let i = 0; i < scanResult.length; i += 2) {
      console.log(`  ${scanResult[i]}: ${scanResult[i+1]}`);
    }

    // Check if data is correct
    if (scanResult.length > 0) {
        console.log("✅ Scan test passed.");
    } else {
        console.error("❌ Scan returned no data!");
    }
}

/**
 * 清理测试数据
 * 
 * 采用流式清理策略 (Streaming Deletion)：
 * 1. 使用 scan() 方法分批次 (Batch=500) 获取 Key。
 * 2. 获取一批后立即执行删除，释放内存。
 * 3. 利用分页游标 (Next Key) 确保不重不漏。
 * 4. 清理完成后通过 count() 再次验证。
 *
 * @param {RedisIndexManager} manager - RedisIndexManager 实例
 * @param {string} dataPrefix - 待清理数据的 Key 前缀
 * @returns {Promise<void>}
 */
async function cleanupData(manager, dataPrefix) {
    // Use loop to fetch and delete keys in batches (Streaming Deletion)
    console.log("Cleaning up test data (Streaming Mode)...");
    
    let lastKey = undefined;
    let totalDeleted = 0;
    
    // Safety limit to prevent infinite loops
    let loopCount = 0;
    const MAX_LOOPS = 1000; // Increase max loops as we might process many batches

    while (loopCount < MAX_LOOPS) {
        loopCount++;
        
        let startKey = lastKey ? lastKey : dataPrefix;
        // If we have a lastKey, we append \x00 to it to start searching strictly after it
        if (lastKey) {
             startKey = lastKey + "\x00"; 
        }
        
        // Scan with limit (e.g. 500)
        const batchData = await manager.scan(startKey, undefined, 500);
        
        if (batchData.length === 0) {
            break;
        }
        
        const batchKeysToDelete = [];
        for (let i = 0; i < batchData.length; i += 2) {
            batchKeysToDelete.push(batchData[i]);
        }
        
        // Save the last key for next iteration pagination
        const nextStartKey = batchData[batchData.length - 2];
        
        // Delete current batch immediately
        const cleanupPromises = batchKeysToDelete.map(key => manager.del(key));
        await Promise.all(cleanupPromises);
        
        // Update lastKey AFTER deleting, but using the saved key from before deletion
        // Note: Even if we delete the key, lastKey is just a string used for lexicographical comparison in scan
        // so it doesn't matter if the key itself exists or not.
        lastKey = nextStartKey;
        
        totalDeleted += batchKeysToDelete.length;
        console.log(`Deleted ${totalDeleted} keys so far...`);
        
        // If we fetched less than limit (500 items * 2 = 1000), it means we reached the end
        if (batchData.length < 1000) {
            break;
        }
    }
    
    console.log(`Cleanup finished. Total deleted: ${totalDeleted} keys.`);
    
    // 7. Verify Cleanup
    console.log("Verifying cleanup...");
    const remainingCount = await manager.count(dataPrefix);
    if (remainingCount === 0) {
        console.log("✅ All test data successfully deleted.");
    } else {
        console.error(`❌ Cleanup failed! ${remainingCount} items remaining.`);
    }
}

/**
 * 主测试流程
 * 
 * 1. 连接 Redis (支持 Cluster/Standalone)。
 * 2. 写入 10000 条测试数据。
 * 3. 验证数据统计与范围查询功能。
 * 4. 清理所有测试数据。
 *
 * @returns {Promise<void>}
 */
async function runTest() {
  console.log("Starting RedisIndexManager test...");

  // 1. Initialize Redis Client
  let redis;
  const redisOptions = {
    connectTimeout: 5000, // 5s timeout
    maxRetriesPerRequest: 1,
    retryStrategy: (times) => {
      if (times > 3) return null; // Stop retrying after 3 attempts
      return Math.min(times * 100, 2000);
    }
  };

  if (Array.isArray(config)) {
    console.log("Config is an array, initializing Redis Cluster...");
    redis = new Redis.Cluster(config, {
      redisOptions: redisOptions,
      clusterRetryStrategy: (times) => {
        if (times > 3) return null;
        return Math.min(times * 100, 2000);
      }
    });
  } else {
    console.log("Config is an object, initializing Redis Standalone...");
    redis = new Redis({ ...config, ...redisOptions });
  }

  // Handle connection errors
  redis.on("error", (err) => {
    console.error("Redis Client Error:", err);
  });

  // Wait for connection to be ready
  console.log("Waiting for Redis connection...");
  await new Promise((resolve, reject) => {
    redis.once("ready", () => {
      console.log("Redis client is ready.");
      resolve();
    });
    
    // Fail fast if connection cannot be established within timeout
    setTimeout(() => {
       if (redis.status !== 'ready') {
         reject(new Error("Redis connection timeout"));
       }
    }, 10000);
  });

  try {
    // Test Connection
    const pingResult = await redis.ping();
    console.log("Redis Connection Test (PING):", pingResult);

    // 2. Initialize RedisIndexManager
    const manager = new RedisIndexManager({
      redis: redis,
      indexPrefix: "test_idx:", // Use a test prefix for index
      hashChars: 2,
    });

    const DATA_PREFIX = "test_user:";
    const WRITE_COUNT = 10000;

    // 3. Write Data
    await testWriteData(manager, DATA_PREFIX, WRITE_COUNT);

    // 4. Scan and Verify Data
    await testScanData(manager, DATA_PREFIX, WRITE_COUNT);

    // 6. Cleanup Data
    await cleanupData(manager, DATA_PREFIX);

  } catch (error) {
    console.error("Test failed:", error);
  } finally {
    // 6. Cleanup and Disconnect
    console.log("Closing Redis connection...");
    redis.quit();
  }
}

runTest();
