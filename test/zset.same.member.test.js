import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { initRedis } from "./tendis.js";

/**
 * Redis key 前缀
 * @type {string}
 */
const PREFIX = "cache_test::";

describe("Redis ZSet Same Member Test", () => {
  let redis;
  const key = `${PREFIX}zset_test`;

  const member = {
    repository: "r18",
    project_id: "zgpa.project21",
    sid: "wechat_0d4d1d05ebf01a0d24abe7023c088440",
  };
  const baseScore = 1772158546345;

  before(async () => {
    redis = initRedis();
    const pingResult = await redis.ping();
    console.log("Redis Connection Test (PING):", pingResult);
  });

  after(async () => {
    // 测试结束后清理数据
    await redis.del(key);
    redis.disconnect();
  });

  it("should update score for same member instead of adding new member", async () => {
    // 清理数据，确保测试环境纯净
    await redis.del(key);
    console.log(`\nCleaned up key: ${key}`);

    /**
     * 循环写入数据
     * Redis ZSet 中 Member 是唯一的，多次写入相同的 Member (即使 Score 不同) 会更新 Score，而不会新增 Member
     */
    console.log("\nStarting to write data...");
    for (let i = 0; i < 10; i++) {
      const currentScore = baseScore + i;
      const memberStr = JSON.stringify(member);

      await redis.zadd(key, currentScore, memberStr);
      console.log(`Write ${i + 1}: score=${currentScore}`);
    }

    // 读取 ZSet 中的数据量
    const count = await redis.zcard(key);
    console.log(`\nZSet Size (zcard): ${count}`);

    // 断言 ZSet 中只有一个元素
    assert.equal(count, 1, "ZSet should only contain 1 member");

    // 取出所有数据并打印
    // WITHSCORES 选项会让结果以 [member1, score1, member2, score2, ...] 的扁平数组形式返回
    const result = await redis.zrange(key, 0, -1, "WITHSCORES");

    console.log("\nZSet All Data (zrange):");
    const [mem, sc] = result;
    console.log(`Member: ${mem}, Score: ${sc}`);

    // 断言 Member 内容正确
    assert.equal(mem, JSON.stringify(member), "Member content should match");

    // 断言 Score 是最后一次写入的值 (baseScore + 9)
    const expectedScore = baseScore + 9;
    // Redis 返回的 score 是字符串，需要转换比较，或者都转字符串
    assert.equal(
      String(sc),
      String(expectedScore),
      "Score should be the last updated value",
    );
  });
});
