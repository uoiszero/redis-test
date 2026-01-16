import Redis from "ioredis";

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
});

/**
 * 计算两个字符串的公共前缀
 */
function getCommonPrefix(str1, str2) {
  let i = 0;
  while (i < str1.length && i < str2.length && str1[i] === str2[i]) {
    i++;
  }
  return str1.substring(0, i);
}

/**
 * 范围扫描函数
 * @param {string} keyStart - 起始 Key (字典序包含)
 * @param {string} keyEnd - 结束 Key (字典序包含)
 * @param {number} limit - 返回的最大数量
 */
async function scanRange(keyStart, keyEnd, limit) {
  return new Promise((resolve, reject) => {
    const results = [];
    
    // 1. 智能推导 MATCH 模式
    // 通过计算 start 和 end 的公共前缀，我们可以缩小 Redis 的扫描范围，大幅提升性能
    // 例如：start="user_001", end="user_100" -> prefix="user_" -> match="user_*"
    const commonPrefix = getCommonPrefix(keyStart, keyEnd);
    const matchPattern = commonPrefix.length > 0 ? `${commonPrefix}*` : "*";
    
    console.log(`[Scan] Range: [${keyStart}, ${keyEnd}]`);
    console.log(`[Scan] Derived Match Pattern: "${matchPattern}"`);

    // 2. 创建扫描流
    const stream = redis.scanStream({
      match: matchPattern,
      count: 1000 // 每次批处理建议数量
    });

    let isEnded = false;

    // 辅助函数：清理并返回
    const finish = () => {
      if (isEnded) return;
      isEnded = true;
      stream.destroy(); // 停止扫描
      resolve(results);
    };

    stream.on("data", (keys) => {
      if (isEnded) return;

      for (const key of keys) {
        // 3. 应用层精确过滤：字典序比较
        // 注意：Redis 的 SCAN 是无序的，所以我们必须在应用层检查每个 Key 是否在范围内
        if (key >= keyStart && key <= keyEnd) {
          results.push(key);
          
          // 4. 达到 Limit 后立即停止
          if (results.length >= limit) {
            finish();
            return;
          }
        }
      }
    });

    stream.on("end", () => {
      finish();
    });

    stream.on("error", (err) => {
      isEnded = true;
      reject(err);
    });
  });
}

// --- 测试代码 ---
async function main() {
  try {
    // 1. 准备测试数据
    console.log("正在写入测试数据...");
    const pipeline = redis.pipeline();
    // 写入 user_000 到 user_199
    for (let i = 0; i < 200; i++) {
      const suffix = i.toString().padStart(3, "0"); // 001, 002...
      pipeline.set(`user_${suffix}`, i);
    }
    // 写入一些干扰数据
    pipeline.set("apple", 1);
    pipeline.set("banana", 1);
    pipeline.set("video_001", 1);
    
    await pipeline.exec();
    console.log("测试数据写入完成");

    // 2. 执行范围查询
    // 目标：查找 user_050 到 user_060 之间的数据，限制返回 5 条
    const start = "user_050";
    const end = "user_060";
    const limit = 5;

    console.log(`\n开始查询: ${start} ~ ${end}, limit=${limit}`);
    const keys = await scanRange(start, end, limit);
    
    console.log("\n查询结果:");
    // 对结果排序以便展示（注意：scanRange 返回的结果不保证顺序，因为 scan 本身是乱序的）
    // 如果业务需要有序结果，需要在这里 .sort()
    keys.sort().forEach(k => console.log(k));
    
    console.log(`\n总共返回: ${keys.length} 条`);

  } catch (err) {
    console.error("Error:", err);
  } finally {
    redis.disconnect();
  }
}

main();
