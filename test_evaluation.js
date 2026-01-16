
import { RedisIndexManager } from "./RedisIndexManager/redis_index_manager_optimized.js";
import assert from "assert";

// Mock Redis Implementation
class MockRedis {
  constructor() {
    this.data = new Map(); // key -> value
    this.zsets = new Map(); // key -> Set(member)
    this.status = "ready";
  }

  defineCommand(name, def) {
    this[name] = async (...args) => {
      if (name === "addIndex") {
        const key = args[0];
        const bucketKey = args[1];
        const value = args[2];
        
        this.data.set(key, value);
        if (!this.zsets.has(bucketKey)) this.zsets.set(bucketKey, new Set());
        this.zsets.get(bucketKey).add(key);
      }
      if (name === "delIndex") {
        const key = args[0];
        const bucketKey = args[1];
        this.data.delete(key);
        if (this.zsets.has(bucketKey)) this.zsets.get(bucketKey).delete(key);
      }
    };
  }

  pipeline() {
    const queue = [];
    const mockPipeline = {
      set: (k, v) => { queue.push({ op: 'set', args: [k, v] }); return mockPipeline; },
      zadd: (k, s, m) => { queue.push({ op: 'zadd', args: [k, m] }); return mockPipeline; },
      del: (k) => { queue.push({ op: 'del', args: [k] }); return mockPipeline; },
      zrem: (k, m) => { queue.push({ op: 'zrem', args: [k, m] }); return mockPipeline; },
      zrangebylex: (k, min, max, ...opts) => { queue.push({ op: 'zrangebylex', args: [k, min, max, opts] }); return mockPipeline; },
      zlexcount: (k, min, max) => { queue.push({ op: 'zlexcount', args: [k, min, max] }); return mockPipeline; },
      exec: async () => {
        return queue.map(cmd => {
          try {
            if (cmd.op === 'set') {
              this.data.set(cmd.args[0], cmd.args[1]);
              return [null, 'OK'];
            }
            if (cmd.op === 'zadd') {
              const [key, member] = cmd.args;
              if (!this.zsets.has(key)) this.zsets.set(key, new Set());
              this.zsets.get(key).add(member);
              return [null, 1];
            }
            if (cmd.op === 'del') {
              this.data.delete(cmd.args[0]);
              return [null, 1];
            }
            if (cmd.op === 'zrem') {
              const [key, member] = cmd.args;
              if (this.zsets.has(key)) this.zsets.get(key).delete(member);
              return [null, 1];
            }
            if (cmd.op === 'zrangebylex') {
              const [key, min, max, opts] = cmd.args;
              if (!this.zsets.has(key)) return [null, []];
              
              let members = Array.from(this.zsets.get(key)).sort();
              
              // 简化的字典序比较
              const cleanMin = min.replace('[', '');
              const cleanMax = max.replace('[', '');
              
              // 简单的字符串比较
              members = members.filter(m => m >= cleanMin);
              if (!max.includes('\\xff')) {
                members = members.filter(m => m <= cleanMax);
              }

              if (opts && opts[0] === 'LIMIT') {
                const offset = opts[1];
                const count = opts[2];
                members = members.slice(offset, offset + count);
              }
              return [null, members];
            }
            if (cmd.op === 'zlexcount') {
               const [key, min, max] = cmd.args;
               if (!this.zsets.has(key)) return [null, 0];
               let members = Array.from(this.zsets.get(key));
               const cleanMin = min.replace('[', '');
               const cleanMax = max.replace('[', '');
               members = members.filter(m => m >= cleanMin);
               if (!max.includes('\\xff')) {
                 members = members.filter(m => m <= cleanMax);
               }
               return [null, members.length];
            }
            return [null, null];
          } catch (e) {
            return [e, null];
          }
        });
      }
    };
    return mockPipeline;
  }

  async mget(keys) {
    return keys.map(k => this.data.get(k) || null);
  }
}

const redis = new MockRedis();
const TEST_PREFIX = "test_idx:";
const DATA_PREFIX = "test_user:";

async function runTests() {
  console.log("Starting RedisIndexManager Evaluation Tests...");

  // Mock 模式不需要清理数据，因为每次都是新的 MockRedis

  const manager = new RedisIndexManager({
    redis: redis,
    indexPrefix: TEST_PREFIX,
    hashChars: 1, // 使用较少的分桶以便测试
    scanBatchSize: 2, // 小批次测试分页逻辑
  });

  try {
    // 1. 测试 Add
    console.log("\n[Test 1] Testing Add...");
    const users = [
      { id: "1001", name: "Alice" },
      { id: "1002", name: "Bob" },
      { id: "1003", name: "Charlie" },
      { id: "1004", name: "David" },
      { id: "1005", name: "Eve" },
    ];

    for (const user of users) {
      await manager.add(`${DATA_PREFIX}${user.id}`, JSON.stringify(user));
    }
    console.log("✓ Added 5 users");

    // 验证 Redis 中是否存在数据
    const exists = redis.data.has(`${DATA_PREFIX}1001`);
    assert.strictEqual(exists, true, "Data key should exist in Redis");

    // 2. 测试 Scan (全量)
    console.log("\n[Test 2] Testing Scan (Full Range)...");
    const allUsers = await manager.scan(`${DATA_PREFIX}1000`, `${DATA_PREFIX}1006`);
    // 结果格式: [key, val, key, val...]
    assert.strictEqual(allUsers.length, 10, "Should return 10 items (5 keys + 5 values)");
    console.log("✓ Scan returned correct number of items");

    // 验证排序
    const firstKey = allUsers[0];
    const lastKey = allUsers[allUsers.length - 2];
    assert.strictEqual(firstKey, `${DATA_PREFIX}1001`, "First key match");
    assert.strictEqual(lastKey, `${DATA_PREFIX}1005`, "Last key match");
    console.log("✓ Results are sorted");

    // 3. 测试 Scan (Limit)
    console.log("\n[Test 3] Testing Scan (With Limit)...");
    const limitedUsers = await manager.scan(`${DATA_PREFIX}1000`, `${DATA_PREFIX}1006`, 3);
    assert.strictEqual(limitedUsers.length, 6, "Should return 6 items (3 keys + 3 values)");
    assert.strictEqual(limitedUsers[0], `${DATA_PREFIX}1001`);
    assert.strictEqual(limitedUsers[4], `${DATA_PREFIX}1003`);
    console.log("✓ Limit works correctly");

    // 4. 测试 Count
    console.log("\n[Test 4] Testing Count...");
    const count = await manager.count(`${DATA_PREFIX}1000`, `${DATA_PREFIX}1006`);
    assert.strictEqual(count, 5, "Count should be 5");
    console.log("✓ Count is correct");

    // 5. 测试 Del
    console.log("\n[Test 5] Testing Del...");
    await manager.del(`${DATA_PREFIX}1003`);
    const afterDelCount = await manager.count(`${DATA_PREFIX}1000`, `${DATA_PREFIX}1006`);
    assert.strictEqual(afterDelCount, 4, "Count should be 4 after deletion");
    
    const verifyDel = redis.data.has(`${DATA_PREFIX}1003`);
    assert.strictEqual(verifyDel, false, "Data key should be deleted");
    console.log("✓ Delete works correctly");

    // 6. 测试无 EndKey 自动推导
    console.log("\n[Test 6] Testing Auto Range Inference...");
    const autoRange = await manager.scan(`${DATA_PREFIX}`);
    assert.strictEqual(autoRange.length, 8, "Should return remaining 4 users"); // 4 users * 2
    console.log("✓ Auto range inference works");

    console.log("\n🎉 All tests passed successfully!");
  } catch (err) {
    console.error("\n❌ Test Failed:", err);
  }
}

runTests();
