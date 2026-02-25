import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert";
import Redis from "ioredis";

const config = {
  host: "localhost",
  port: 6379,
};

const redis = new Redis(config);

const testKeys = [];

function addTestKey(key) {
  testKeys.push(key);
}

describe("Redis连接测试", () => {
  test("应该能够成功连接到Redis服务器", async () => {
    const result = await redis.ping();
    assert.strictEqual(result, "PONG");
  });
});

describe("Redis SET操作测试", () => {
  test("应该能够正常设置和获取值", async () => {
    const key = "test_key";
    const value = "test_value";
    addTestKey(key);
    await redis.set(key, value);
    const result = await redis.get(key);
    assert.strictEqual(result, value);
  });
});

describe("Redis SET with TTL测试", () => {
  test("应该能够设置带过期时间的键值对", async () => {
    const key = "test_key_ttl";
    const value = "test_value_ttl";
    const ttl = 10;
    addTestKey(key);
    await redis.set(key, value, "EX", ttl);
    const result = await redis.get(key);
    const actualTTL = await redis.ttl(key);
    assert.strictEqual(result, value);
    assert.ok(actualTTL > 0 && actualTTL <= ttl);
  });
});

describe("Redis SET数据类型测试", () => {
  test("SISMEMBER - 应该能够正确判断成员是否存在", async () => {
    const key = "test_set_key";
    addTestKey(key);
    await redis.sadd(key, "member1", "member2", "member3");
    const existsResult = await redis.sismember(key, "member2");
    const notExistsResult = await redis.sismember(key, "member4");
    assert.strictEqual(existsResult, 1);
    assert.strictEqual(notExistsResult, 0);
  });
});

afterEach(async () => {
  for (const key of testKeys) {
    await redis.del(key);
  }
  testKeys.length = 0;
});

after(async () => {
  await redis.quit();
});
