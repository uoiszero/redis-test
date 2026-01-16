import Redis from "ioredis";
import crypto from "crypto";

/**
 * Redis 二级索引管理器 (Hash 分桶版)
 * 
 * 核心变更：
 * 1. 分桶策略：不再使用前缀，而是对 Key 进行 Hash 后取前 N 位 (Hex) 进行分桶。
 *    目的：在海量数据下，强制将索引均匀打散到多个 ZSET 中，避免单桶过大。
 * 2. 查询策略：Scatter-Gather (分散-聚合)。
 *    由于 Hash 打散了数据，范围查询 (Range Scan) 必须并发扫描所有桶，然后在内存中合并排序。
 */
class RedisIndexManager {
  /**
   * @param {Object} options
   * @param {Redis} options.redis - ioredis 实例
   * @param {string} [options.indexPrefix="idx:"] - 索引 Key 的前缀
   * @param {number} [options.hashChars=2] - Hash 分桶取前几位 (Hex)。
   *    - 1位: 16桶 (适合千万级)
   *    - 2位: 256桶 (适合亿级, 推荐)
   *    - 3位: 4096桶 (适合十亿+, 但会导致 Scan 变慢)
   */
  constructor(options) {
    this.redis = options.redis;
    this.indexPrefix = options.indexPrefix || "idx:";
    
    // 参数校验: hashChars 必须是 1 或 2
    // 1位 Hex = 16 桶; 2位 Hex = 256 桶
    // 更多位数会导致 scan 时并发请求过多，性能急剧下降，因此强制限制
    const hashChars = options.hashChars || 2;
    if (hashChars !== 1 && hashChars !== 2) {
      throw new Error("options.hashChars must be 1 or 2");
    }
    this.hashChars = hashChars;
    
    // 预计算所有桶的后缀，用于 Scan 时快速遍历
    // 例如 hashChars=2, buckets=["00", "01", ... "ff"]
    this.buckets = [];
    const maxVal = Math.pow(16, this.hashChars);
    for (let i = 0; i < maxVal; i++) {
      this.buckets.push(i.toString(16).padStart(this.hashChars, "0"));
    }
  }

  /**
   * 内部方法：计算 Key 所属的桶
   */
  _getBucketKey(key) {
    // 使用 MD5 计算 Hash (速度快，分布均匀)
    const hash = crypto.createHash("md5").update(key).digest("hex");
    const bucketSuffix = hash.substring(0, this.hashChars);
    return `${this.indexPrefix}${bucketSuffix}`;
  }

  /**
   * 1. 添加数据 (Add)
   */
  async add(key, value) {
    const bucketKey = this._getBucketKey(key);
    const pipeline = this.redis.pipeline();

    pipeline.set(key, value);
    // Score 固定为 0，利用 Member 字典序
    pipeline.zadd(bucketKey, 0, key);

    await pipeline.exec();
  }

  /**
   * 2. 删除数据 (Delete)
   */
  async del(key) {
    const bucketKey = this._getBucketKey(key);
    const pipeline = this.redis.pipeline();

    pipeline.del(key);
    pipeline.zrem(bucketKey, key);

    await pipeline.exec();
  }

  /**
   * 3. 范围扫描 (Scatter-Gather Scan)
   * 注意：这会向 Redis 发送 16^N 个并发请求，请评估 limit 大小
   * 
   * @param {string} startKey - 起始 Key (包含)
   * @param {string} endKey - 结束 Key (包含)。如果为空，自动推导为 startKey 的前缀结束范围
   * @param {number} limit - 返回数量限制 (1-1000)
   * @param {boolean} [withValues=false] - 是否同时返回对应的 Value
   */
  async scan(startKey, endKey, limit = 100, withValues = false) {
    // 0. 参数校验
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("Limit must be an integer between 1 and 1000");
    }

    // 1. 确定扫描范围
    const lexStart = `[${startKey}`;
    let lexEnd;
    
    if (!endKey) {
      // 假设 startKey="user_001"，如果没传 endKey，我们尝试猜测用户意图是扫描 "user_*"
      // 寻找第一个分隔符
      const match = startKey.match(/^([a-zA-Z0-9]+)(_|:)/);
      if (match) {
        // "user_" -> "user_\xff"
        lexEnd = `[${match[0]}\xff`;
      } else {
        // 如果无法识别前缀，默认扫描到无穷大 (慎用)
        lexEnd = "+";
      }
    } else {
      lexEnd = `[${endKey}`;
    }

    console.log(`[Scan] Range: [${startKey}, ${endKey || "AUTO"}], Buckets: ${this.buckets.length}`);

    // 2. Scatter (分散): 分批次并发查询所有桶
    // 优化：控制并发度，避免一次性发送 256 个请求导致客户端或网络拥塞
    // Redis 官方虽然没有硬性限制 pipeline 长度，但通常建议 50-100 个命令为一批
    const BATCH_SIZE = 50; 
    const results = [];
    
    for (let i = 0; i < this.buckets.length; i += BATCH_SIZE) {
      const bucketBatch = this.buckets.slice(i, i + BATCH_SIZE);
      const pipeline = this.redis.pipeline();
      
      for (const bucketSuffix of bucketBatch) {
        const bucketKey = `${this.indexPrefix}${bucketSuffix}`;
        pipeline.zrangebylex(bucketKey, lexStart, lexEnd, "LIMIT", 0, limit);
      }

      // 执行当前批次
      const batchResults = await pipeline.exec();
      results.push(...batchResults);
    }

    // 3. Gather (聚合): 收集所有桶的结果
    let allKeys = [];
    
    for (const [err, keys] of results) {
      if (err) {
        console.error("Scan error in one bucket:", err);
        continue;
      }
      if (keys && keys.length > 0) {
        allKeys.push(...keys);
      }
    }

    // 4. 内存排序与截断
    // 因为数据散落在不同桶，必须汇总后重新排序
    allKeys.sort(); // 字典序排序

    // 截取全局 Top N
    if (allKeys.length > limit) {
      allKeys = allKeys.slice(0, limit);
    }

    // 5. 如果需要 Value，再进行一次批量回查
    if (!withValues || allKeys.length === 0) {
      return allKeys;
    }

    // 优化：虽然 Limit 限制了 Key 的数量 (Max 1000)，但如果 Value 很大 (如 10KB+)，
    // 一次性 MGET 10MB 数据仍可能阻塞网络或导致 Node.js 瞬间 GC 压力。
    // 这里采用分批并行获取策略，兼顾延迟和稳定性。
    const MGET_BATCH_SIZE = 200;
    const valuePromises = [];
    
    for (let i = 0; i < allKeys.length; i += MGET_BATCH_SIZE) {
      const batchKeys = allKeys.slice(i, i + MGET_BATCH_SIZE);
      valuePromises.push(this.redis.mget(batchKeys));
    }

    // 并行执行所有批次 (通常 1000 个 Key 分 5 批，对 Server 压力很小)
    const batchResults = await Promise.all(valuePromises);
    
    // 拍平结果 (Redis MGET 返回的是数组，Promise.all 返回的是数组的数组)
    const values = batchResults.flat();

    return allKeys.map((k, i) => ({
      key: k,
      value: values[i]
    }));
  }
}

// --- 测试代码 ---

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
});

async function main() {
  // 使用 2 位 Hex 分桶 (256 个桶)
  const manager = new RedisIndexManager({
    redis: redis,
    hashChars: 2 
  });

  try {
    console.log("--- 1. 准备测试数据 (Hash分桶版) ---");
    // 写入 200 条数据，测试它们是否被正确打散和查回
    const prefix = "user";
    const total = 200;
    
    // 模拟批量写入
    for (let i = 0; i < total; i++) {
      const suffix = i.toString().padStart(6, "0");
      await manager.add(`${prefix}_${suffix}`, `val-${i}`);
    }
    
    console.log(`写入完成: ${prefix}_000000 ~ ${prefix}_000199`);

    console.log("\n--- 2. 测试跨桶范围查询 ---");
    // 即使 user_000050 和 user_000051 被哈希到了完全不同的桶
    // scan 方法也应该能把它们找出来并排好序
    const startKey = "user_000050";
    const endKey = "user_000060";
    const limit = 5;
    
    const results = await manager.scan(startKey, endKey, limit, true);
    
    console.log(`查询结果 [${startKey}, ${endKey}], Limit: ${limit}`);
    results.forEach(item => {
      console.log(`  Key: ${item.key} (Hash Bucket: ${manager._getBucketKey(item.key)})`);
    });

    // 验证排序正确性
    if (results.length > 0 && results[0].key === startKey) {
        console.log("验证通过: 结果包含起始Key且排序正确");
    }

  } catch (err) {
    console.error("Error:", err);
  } finally {
    redis.disconnect();
  }
}

main();
