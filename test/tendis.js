import Redis from "ioredis";

const singletonConfig = {
  host: "localhost",
  port: 6379,
};

const clusterConfig = [
  {
    host: "192.168.5.9",
    port: 51002,
  },
  {
    host: "192.168.5.10",
    port: 51002,
  },
  {
    host: "192.168.5.11",
    port: 51002,
  },
];

const mode = process.env.TEST_MODE || "cluster";
console.log(`Running test in ${mode} mode...`);
const config = mode === "cluster" ? clusterConfig : singletonConfig;

/**
 * @returns 初始化redis客户端
 */
export function initRedis() {
  const redisOptions = {
    connectTimeout: 5000, // 5s timeout
    maxRetriesPerRequest: 1,
    enableReadyCheck: false,
    retryStrategy: times => {
      if (times > 3) return null; // Stop retrying after 3 attempts
      return Math.min(times * 100, 2000);
    },
  };

  if (Array.isArray(config)) {
    console.log("Config is an array, initializing Redis Cluster...");
    return new Redis.Cluster(config, {
      redisOptions: redisOptions,
      clusterRetryStrategy: times => {
        if (times > 3) return null;
        return Math.min(times * 100, 2000);
      },
    });
  } else {
    console.log("Config is an object, initializing Redis Standalone...");
    return new Redis({ ...config, ...redisOptions });
  }
}
