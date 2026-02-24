import Redis from "ioredis";

const nodes = [
  { host: "192.168.6.100", port: 51002 },
  { host: "192.168.6.101", port: 51002 },
  { host: "192.168.6.102", port: 51002 },
];

async function checkNodes() {
  console.log("Checking Redis nodes status...");

  for (const node of nodes) {
    const redis = new Redis({
      host: node.host,
      port: node.port,
      connectTimeout: 2000,
      retryStrategy: () => null, // Do not retry
    });

    try {
      console.log(`\nConnecting to ${node.host}:${node.port}...`);
      const info = await redis.info("persistence");
      const loading = info.match(/loading:(\d)/);
      
      if (loading && loading[1] === "1") {
        console.log(`⚠️  Node ${node.host}:${node.port} is LOADING data.`);
        const loadingTime = info.match(/loading_start_time:(\d+)/);
        const totalBytes = info.match(/loading_total_bytes:(\d+)/);
        const loadedBytes = info.match(/loading_loaded_bytes:(\d+)/);
        const eta = info.match(/loading_eta_seconds:(\d+)/);
        
        if (eta) console.log(`   ETA: ${eta[1]} seconds`);
        if (loadedBytes && totalBytes) {
             const progress = (parseInt(loadedBytes[1]) / parseInt(totalBytes[1])) * 100;
             console.log(`   Progress: ${progress.toFixed(2)}%`);
        }
      } else {
        console.log(`✅ Node ${node.host}:${node.port} is READY.`);
      }
    } catch (err) {
      console.error(`❌ Failed to connect to ${node.host}:${node.port}: ${err.message}`);
    } finally {
      redis.disconnect();
    }
  }
}

checkNodes();
