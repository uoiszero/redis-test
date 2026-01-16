/**
 * ioredis 最佳实践示例
 * 包含：连接管理、错误重试、Pipeline 批量操作、Lua 脚本、以及类型安全的 ZADD 操作
 */
import Redis from "ioredis";

// 1. 健壮的连接配置
// 在生产环境中，建议将其封装为单例模块
const redis = new Redis({
  host: "127.0.0.1", // 默认为 localhost
  port: 6379,
  
  // 连接密码
  // password: "your_password",
  
  // 自动重连策略
  // ioredis 默认会无限重试，但我们可以自定义策略
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    console.log(`[Redis] Connection lost. Retrying in ${delay}ms...`);
    return delay;
  },

  // 如果连接失败且重试耗尽，是否抛出错误
  maxRetriesPerRequest: 3,
});

// 事件监听：用于监控连接状态
redis.on("connect", () => console.log("[Redis] Connected"));
redis.on("ready", () => console.log("[Redis] Ready"));
redis.on("error", (err) => console.error("[Redis] Error:", err.message));

async function main() {
  try {
    // 2. 正确的 ZADD 用法
    // 场景：记录带有时间戳的事件
    const key = "events:log";
    const score = Date.now(); // Score 必须是数字
    const member = JSON.stringify({ event: "login", userId: 1001 }); // Member 必须是字符串
    
    // ZADD key score member
    await redis.zadd(key, score, member);
    console.log("ZADD success");

    // 3. Pipeline (管道) - 批量操作提升性能
    // 相比于串行 await，Pipeline 可以将多个命令一次性发送给 Redis
    const pipeline = redis.pipeline();
    for (let i = 0; i < 5; i++) {
      pipeline.set(`temp:key:${i}`, `value-${i}`, "EX", 60);
    }
    const results = await pipeline.exec();
    console.log(`Pipeline executed. Results count: ${results.length}`);

    // 4. Lua 脚本 - 原子操作
    // 场景：实现一个“检查并设置”的原子逻辑 (如果 key 不存在则设置，存在则返回旧值)
    // 这是一个简单的例子，实际上 SET NX 已经支持，但这里为了演示 Lua
    const luaScript = `
      local key = KEYS[1]
      local value = ARGV[1]
      local existing = redis.call("get", key)
      if not existing then
        redis.call("set", key, value)
        return nil
      else
        return existing
      end
    `;

    // 定义自定义命令
    redis.defineCommand("checkAndSet", {
      numberOfKeys: 1,
      lua: luaScript,
    });

    // 调用自定义命令 (注意：defineCommand 会在 Redis 实例上添加新方法)
    // @ts-ignore - JS 中需要忽略类型检查，或者扩展类型定义
    const luaResult = await redis.checkAndSet("my-lua-key", "lua-value");
    console.log("Lua script result:", luaResult);

  } catch (err) {
    console.error("Execution failed:", err);
  } finally {
    // 关闭连接
    redis.disconnect();
  }
}

main();
