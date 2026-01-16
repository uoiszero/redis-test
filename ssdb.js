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

async function scanStream() {
  return new Promise((resolve, reject) => {
    let result;
    //获取所有以 test:: 开头的 key
    const stream = redis.scanStream({
      match: "test:*"
    });

    stream.on("data", (resultKeys, val) => {
      stream.pause();
      console.log(resultKeys, val);
      stream.resume();
    });
    stream.on("end", () => {
      return resolve();
    })
  })
}

async function scan() {
  let result = await redis.scan(0, "MATCH", "test:*");
  console.log(result);
}

(async () => {
  await scanStream();
  redis.disconnect();
})();