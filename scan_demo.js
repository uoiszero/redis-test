import Redis from "ioredis";

// 创建 Redis 连接
const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
});

/**
 * 方法一（推荐）：使用 scanStream
 * 适用于数据量大（如 100万+）的场景
 * 优点：流式处理，不会阻塞 Redis 主线程，也不会瞬间占用 Node.js 过多内存
 */
async function findKeysUsingStream() {
  console.log("--- 使用 scanStream 查找 ---");
  
  const stream = redis.scanStream({
    match: "user_*", // 匹配 user_ 开头的 key
    count: 1000      // 每次与 Redis 交互尝试获取的数量（提示 Redis 每次扫描多少个元素，非严格返回数量）
  });

  let totalFound = 0;

  stream.on("data", (resultKeys) => {
    // resultKeys 是当前批次找到的 key 数组
    // 注意：即使没有找到 key，resultKeys 也可能为空数组，需要判空
    if (resultKeys.length > 0) {
      totalFound += resultKeys.length;
      
      // 在这里处理你的业务逻辑，例如：
      resultKeys.forEach(key => {
        // console.log("Found key:", key);
      });
      
      console.log(`本批次找到 ${resultKeys.length} 个 key`);
    }
  });

  stream.on("end", () => {
    console.log(`扫描结束，总共找到: ${totalFound} 个 key`);
    // 演示结束，关闭连接
    redis.disconnect();
  });

  stream.on("error", (err) => {
    console.error("扫描出错:", err);
    redis.disconnect();
  });
}

/**
 * 方法二（仅供理解原理）：手动循环调用 scan
 * 实际上 scanStream 内部就是这样封装的
 */
async function findKeysManualScan() {
  let cursor = "0";
  const foundKeys = [];
  
  do {
    // scan 方法返回两个值：[新游标, [key数组]]
    const result = await redis.scan(cursor, "MATCH", "user_*", "COUNT", 1000);
    
    cursor = result[0];
    const keys = result[1];
    
    if (keys.length > 0) {
      foundKeys.push(...keys);
    }
  } while (cursor !== "0"); // 当游标回到 "0" 时表示遍历结束
  
  console.log(`Manual Scan found ${foundKeys.length} keys`);
}

/**
 * ❌ 错误示范：redis.keys("user_*")
 * 
 * 为什么不能用？
 * 1. 它是 O(N) 操作，会遍历所有 100万+ 数据。
 * 2. Redis 是单线程的，执行 keys 指令期间，整个 Redis 会被阻塞，
 *    无法响应任何其他请求（如线上用户的正常读写），导致严重的生产事故。
 */

// 执行推荐的方法
findKeysUsingStream();
