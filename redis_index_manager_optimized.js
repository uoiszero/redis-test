import Redis from "ioredis";
import crypto from "crypto";

/**
 * Redis 二级索引管理器 (优化版)
 *
 * 该类通过 Hash 分桶策略实现了高效的 Redis 二级索引管理，解决了海量数据下的索引热点问题。
 *
 * 核心特性：
 * 1. **分桶策略**：对 Key 进行 Hash 后取前 N 位 (Hex) 分桶，将索引均匀打散到多个 ZSET 中。
 * 2. **Scatter-Gather 查询**：范围查询时并发扫描所有相关桶，并在内存中进行归并排序。
 * 3. **内存保护**：Scan 采用分批合并 (Incremental Merge) 策略，防止 Limit 放大效应导致的内存溢出。
 * 4. **原子操作**：使用 Lua 脚本保证索引与源数据的一致性。
 *
 * @example
 * const manager = new RedisIndexManager({ redis: redisClient, hashChars: 2 });
 * await manager.add("user:1001", JSON.stringify({ name: "Alice" }));
 * const users = await manager.scan("user:1000", "user:1005", 10);
 */
export class RedisIndexManager {
  /**
   * @param {Object} options
   * @param {Redis} options.redis - ioredis 实例
   * @param {string} [options.indexPrefix="idx:"] - 索引 Key 的前缀
   * @param {number} [options.hashChars=2] - Hash 分桶取前几位 (Hex)
   */
  constructor(options) {
    this.redis = options.redis;
    this.indexPrefix = options.indexPrefix || "idx:";

    const hashChars = options.hashChars || 2;
    if (hashChars !== 1 && hashChars !== 2) {
      throw new Error("options.hashChars must be 1 or 2");
    }
    this.hashChars = hashChars;

    // 配置项: 批处理大小
    this.SCAN_BATCH_SIZE = options.scanBatchSize || 50;
    this.MGET_BATCH_SIZE = options.mgetBatchSize || 200;
    
    this.buckets = [];
    const maxVal = Math.pow(16, this.hashChars);
    for (let i = 0; i < maxVal; i++) {
      this.buckets.push(i.toString(16).padStart(this.hashChars, "0"));
    }

    // 定义 Lua 脚本 (检查是否支持 defineCommand，方便测试 Mock)
    if (typeof this.redis.defineCommand === "function") {
      this.redis.defineCommand("addIndex", {
        numberOfKeys: 2,
        lua: `
          redis.call('SET', KEYS[1], ARGV[1])
          redis.call('ZADD', KEYS[2], 0, KEYS[1])
        `,
      });

      this.redis.defineCommand("delIndex", {
        numberOfKeys: 2,
        lua: `
          redis.call('DEL', KEYS[1])
          redis.call('ZREM', KEYS[2], KEYS[1])
        `,
      });
    }
  }

  /**
   * 计算 Key 所属的桶名
   *
   * @private
   * @param {string} key - 原始 Key
   * @returns {string} 桶的完整 Key (prefix + hashSuffix)
   */
  _getBucketKey(key) {
    const hash = crypto.createHash("md5").update(key).digest("hex");
    const bucketSuffix = hash.substring(0, this.hashChars);
    return `${this.indexPrefix}${bucketSuffix}`;
  }

  /**
   * 添加或更新数据及其索引 (原子操作)
   *
   * 使用 Lua 脚本同时更新 KV 数据和 ZSET 索引，确保两者的一致性。
   * 如果 Key 已存在，将覆盖原有 Value 并更新索引（Score 固定为 0）。
   *
   * @param {string} key - 数据的唯一标识 (如 "user:1001")
   * @param {string} value - 数据内容 (字符串或序列化后的 JSON)
   * @returns {Promise<void>}
   */
  async add(key, value) {
    const bucketKey = this._getBucketKey(key);
    // 如果支持 Lua 脚本则使用，否则降级为 Pipeline (兼容 Mock)
    if (typeof this.redis.addIndex === "function") {
      await this.redis.addIndex(key, bucketKey, value);
    } else {
      console.warn(
        "[RedisIndexManager] Lua scripts not supported. Falling back to non-atomic pipeline. Data consistency is NOT guaranteed."
      );
      const pipeline = this.redis.pipeline();
      pipeline.set(key, value);
      pipeline.zadd(bucketKey, 0, key);
      await pipeline.exec();
    }
  }

  /**
   * 删除数据及其索引 (原子操作)
   *
   * 使用 Lua 脚本同时删除 KV 数据和 ZSET 中的索引条目。
   *
   * @param {string} key - 待删除数据的唯一标识
   * @returns {Promise<void>}
   */
  async del(key) {
    const bucketKey = this._getBucketKey(key);
    if (typeof this.redis.delIndex === "function") {
      await this.redis.delIndex(key, bucketKey);
    } else {
      console.warn(
        "[RedisIndexManager] Lua scripts not supported. Falling back to non-atomic pipeline. Data consistency is NOT guaranteed."
      );
      const pipeline = this.redis.pipeline();
      pipeline.del(key);
      pipeline.zrem(bucketKey, key);
      await pipeline.exec();
    }
  }

  /**
   * 范围扫描 (Scatter-Gather Scan) - 内存优化版
   *
   * 并发扫描所有分桶，查找符合字典序范围 [startKey, endKey] 的 Keys。
   * 采用分批合并策略，有效控制内存占用，避免 OOM。
   *
   * @param {string} startKey - 起始 Key (包含)，例如 "user:1000"
   * @param {string} [endKey] - 结束 Key (包含)。如果未提供，必须保证 startKey 能推导出前缀范围。
   * @param {number} [limit=100] - 返回结果的最大数量 (1-1000)。注意：这是全局 Limit。
   * @returns {Promise<Array<string>>} Key 和 Value 交替排列的扁平数组 [key1, val1, key2, val2...]
   * @throws {Error} 如果 limit 不合法或无法推导 endKey 范围时抛出异常
   */
  async scan(startKey, endKey, limit = 100) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("Limit must be an integer between 1 and 1000");
    }

    const lexStart = `[${startKey}`;
    let lexEnd;

    if (!endKey) {
      const match = startKey.match(/^([a-zA-Z0-9]+)(_|:|-|\/|#)/);
      if (match) {
        lexEnd = `[${match[0]}\xff`;
      } else {
        // 安全改进：无法推导时抛出错误
        throw new Error(
          "Cannot infer endKey from startKey. Please provide an explicit endKey to avoid full scan."
        );
      }
    } else {
      lexEnd = `[${endKey}`;
    }

    // console.log(`[Scan Optimized] Range: [${startKey}, ${endKey || "AUTO"}], Limit: ${limit}`);

    let allKeys = [];

    for (let i = 0; i < this.buckets.length; i += this.SCAN_BATCH_SIZE) {
      const bucketBatch = this.buckets.slice(i, i + this.SCAN_BATCH_SIZE);
      const pipeline = this.redis.pipeline();

      for (const bucketSuffix of bucketBatch) {
        const bucketKey = `${this.indexPrefix}${bucketSuffix}`;
        pipeline.zrangebylex(bucketKey, lexStart, lexEnd, "LIMIT", 0, limit);
      }

      const batchResults = await pipeline.exec();

      // 优化：分批合并 (Incremental Merge)
      let batchKeys = [];
      for (const [err, keys] of batchResults) {
        if (err) {
          console.error("Scan error:", err);
          continue;
        }
        if (keys && keys.length > 0) {
          batchKeys.push(...keys);
        }
      }

      if (batchKeys.length > 0) {
        // 策略：将新的一批数据加入总集，然后立即排序并截断
        // 这样内存中永远只保留 Limit + BatchSize*Limit 的数据量
        allKeys = allKeys.concat(batchKeys);
        allKeys.sort();

        if (allKeys.length > limit) {
          allKeys = allKeys.slice(0, limit);
        }
      }
    }

    if (allKeys.length === 0) {
      return [];
    }

    const valuePromises = [];
    
    for (let i = 0; i < allKeys.length; i += this.MGET_BATCH_SIZE) {
      const batchKeys = allKeys.slice(i, i + this.MGET_BATCH_SIZE);
      valuePromises.push(this.redis.mget(batchKeys));
    }

    const batchResults = await Promise.all(valuePromises);
    const values = batchResults.flat();

    // 拍平为 [key1, val1, key2, val2, ...]
    const result = [];
    for (let i = 0; i < allKeys.length; i++) {
      result.push(allKeys[i], values[i]);
    }
    return result;
  }
}
