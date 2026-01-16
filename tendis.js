/**
 Created by alex on 2024/12/31
 */
import Redis from "ioredis";

async function connect() {
  return new Promise((resolve, reject) => {
    const redis = new Redis.Cluster([
      {
        host: "192.168.6.1",
        port: 51002
      },
      {
        host: "192.168.6.2",
        port: 51002
      },
      {
        host: "192.168.6.3",
        port: 51002
      }
    ]);
    redis.on("connect", () => {
      console.log("connect");
      //resolve(redis);
    })
    redis.on("ready", () => {
      console.log("ready");
    })
    redis.on("error", err => {
      console.log("error: ", err);
      reject(err);
    })
    redis.on("close", ()=>{
      console.log("close");
    })
    redis.on("end", ()=>{
        console.log("end");
    })
    redis.on("wait", ()=>{
        console.log("wait");
    })
  });
}

async function scan() {

}

async function main(redis) {
  let result;
  result = await redis.get("test::foo");
  console.log(result);
}

(async () => {
  const redis = await connect();
  let result = await redis.get("test::foo");
  console.log(result);
})();