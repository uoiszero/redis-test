import Redis from "ioredis";

const redis = new Redis({
  host: "192.168.5.115",
  port: 8888
});

async function set() {
  let result;
  result = await redis.set("test::foo", 1, "EX", 60);
  console.log(result);
  result = await redis.set("test::bar", 2);
  console.log(result);
}

async function get() {
  let result;
  result = await redis.get("test::foo1");
  console.log(result);
  result = await redis.get("test::bar");
  console.log(result);
}

async function mset() {
  let result = await redis.mset({ "test::foo1": 1.1, "test::foo2": 2.2 });
  console.log(result);
}

(async () => {
  let i = 0;
  while (i < 10) {
    const timestamp = Date.now();
    const key = `test::${i}`;
    const val = {
      timestamp,
      value: i
    }
    let result = await redis.zadd("update-es", key, val);
    console.log(result);
  }
  redis.disconnect();
})();