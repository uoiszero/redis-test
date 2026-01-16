import { RedisIndexManager } from "./RedisIndexManager/redis_index_manager_optimized.js";

// Mock Pipeline
class MockPipeline {
  constructor() {
    this.commands = [];
  }
  
  zrangebylex(...args) {
    this.commands.push({ cmd: 'zrangebylex', args });
    return this; 
  }

  set() { return this; }
  zadd() { return this; }
  del() { return this; }
  zrem() { return this; }

  async exec() {
    await new Promise(r => setTimeout(r, 10));

    return this.commands.map(cmd => {
      if (cmd.cmd === 'zrangebylex') {
        const keys = [];
        for (let i = 0; i < 1000; i++) {
            keys.push(`user_${Math.random().toString(36).slice(2)}`);
        }
        return [null, keys];
      }
      return [null, "OK"];
    });
  }
}

// Mock Redis Client
class MockRedis {
  pipeline() {
    return new MockPipeline();
  }
  
  async mget(keys) {
    return keys.map(() => "mock_value");
  }
  
  // 模拟 defineCommand
  defineCommand() {}
}

async function runTest() {
  console.log("--- 开始优化版性能测试 ---");
  
  const mockRedis = new MockRedis();
  const manager = new RedisIndexManager({
    redis: mockRedis,
    hashChars: 2 
  });

  console.log("配置: 256 Buckets, 模拟每个桶匹配 1000 条数据");
  
  const start = process.hrtime.bigint();
  
  // 模拟一次大范围 Scan
  const result = await manager.scan("user_", "user_\xff", 1000, false);
  
  const end = process.hrtime.bigint();
  const duration = Number(end - start) / 1e6; // ms

  console.log(`\nScan (Optimized) 完成!`);
  console.log(`耗时: ${duration.toFixed(2)} ms`);
  console.log(`返回结果数量: ${result.length}`);
  
  const used = process.memoryUsage();
  console.log(`\n内存占用:`);
  console.log(`RSS: ${(used.rss / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Heap Used: ${(used.heapUsed / 1024 / 1024).toFixed(2)} MB`);
}

runTest();
