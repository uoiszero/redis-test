import Redis from "ioredis";

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
});

/**
 * 场景：海量数据(1000万+)下的高性能范围查询
 * 方案：使用 Sorted Set (ZSET) 构建二级索引
 * 
 * 核心原理：
 * 1. Redis 的 Keys 本身是无序的 (Hash Table)。
 * 2. ZSET 是有序的 (Skip List)。
 * 3. 我们创建一个 ZSET 专门用来存储 Key 的索引，将所有 Score 设为 0，
 *    利用 ZSET 的字典序 (Lexicographical) 特性进行范围查找。
 */

const INDEX_KEY = "sys:keys_index";

// 模拟业务：插入数据的同时更新索引
async function saveData(key, value) {
  // 使用 pipeline 保证原子性（或使用 multi）
  const pipeline = redis.pipeline();
  
  // 1. 存实际数据
  pipeline.set(key, value);
  
  // 2. 更新索引 (Score 固定为 0，仅利用 Member 排序)
  pipeline.zadd(INDEX_KEY, 0, key);
  
  await pipeline.exec();
}

/**
 * 高性能范围查询 (O(log N) + M)
 * 相比 SCAN 的 O(N)，性能提升巨大
 */
async function searchRangeByIndex(keyStart, keyEnd, limit) {
  console.log(`[Index Search] Range: [${keyStart}, ${keyEnd}], Limit: ${limit}`);
  
  // ZRANGEBYLEX key [min [max LIMIT offset count
  // '[' 表示包含 (inclusive)，'(' 表示不包含 (exclusive)
  // 如果是无穷大/小，可以使用 '+' 和 '-'
  
  const results = await redis.zrangebylex(
    INDEX_KEY, 
    `[${keyStart}`, 
    `[${keyEnd}`, 
    "LIMIT", 
    0, 
    limit
  );
  
  return results;
}

// --- 测试 ---
async function main() {
  try {
    console.log("正在构建索引数据...");
    // 插入一些乱序数据
    const testKeys = [
      "apple", "banana", 
      "user_001", "user_002", "user_003", "user_050", "user_099", "user_100",
      "video_001", "zebra"
    ];
    
    for (const k of testKeys) {
      await saveData(k, "some-value");
    }
    
    // 查询 user_ 开头的所有数据
    // 技巧：user_ 的结束范围通常可以构造为 user_ + \xff (不可见字符，ASCII码最大)
    // 或者明确指定业务上的结束 Key，如 user_999
    
    // 示例1：明确范围
    const result1 = await searchRangeByIndex("user_001", "user_050", 5);
    console.log("\n查询结果 (user_001 ~ user_050):");
    console.log(result1);

    // 示例2：前缀查询技巧 (user_ ~ user_ + \xff)
    // \xff 是 8位二进制 11111111，在字符串比较中通常比任何常规字符都大
    const result2 = await searchRangeByIndex("user_", "user_\xff", 100);
    console.log("\n查询结果 (所有 user_ 开头):");
    console.log(result2);

  } catch (err) {
    console.error(err);
  } finally {
    redis.disconnect();
  }
}

main();
