import { Redis } from "@upstash/redis";

const QUEUE_KEY = "ezclips:jobs";
const POLL_INTERVAL = 5000;

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const API_URL = process.env.API_URL || "http://localhost:5000";
const WORKER_SECRET = process.env.WORKER_SECRET;

async function sendCallback(payload) {
  try {
    const response = await fetch(`${API_URL}/api/worker/callback`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Worker-Secret": WORKER_SECRET || "",
      },
      body: JSON.stringify(payload),
    });
    
    if (!response.ok) {
      console.error("[Worker] Callback failed:", await response.text());
    }
  } catch (error) {
    console.error("[Worker] Failed to send callback:", error);
  }
}

async function processJob(job) {
  console.log(`[Worker] Processing job ${job.jobId}`);
  
  await sendCallback({ jobId: job.jobId, status: "processing", progressPct: 10, step: "Downloading VOD..." });
  await new Promise((r) => setTimeout(r, 2000));
  
  await sendCallback({ jobId: job.jobId, status: "processing", progressPct: 30, step: "Analyzing audio waveforms..." });
  await new Promise((r) => setTimeout(r, 2000));
  
  await sendCallback({ jobId: job.jobId, status: "processing", progressPct: 50, step: "Detecting highlight moments..." });
  await new Promise((r) => setTimeout(r, 2000));
  
  await sendCallback({ jobId: job.jobId, status: "processing", progressPct: 70, step: "Generating clip thumbnails..." });
  await new Promise((r) => setTimeout(r, 2000));
  
  await sendCallback({ jobId: job.jobId, status: "processing", progressPct: 90, step: "Rendering final clips..." });
  await new Promise((r) => setTimeout(r, 2000));

  const mockClips = [
    { startSec: 120, endSec: 150, title: "Epic Clutch Play", engagementScore: 95, reason: "High kill count + chat spike", game: "Valorant" },
    { startSec: 450, endSec: 480, title: "Insane Headshot Streak", engagementScore: 92, reason: "Rapid kills + viewer engagement", game: "Valorant" },
    { startSec: 890, endSec: 920, title: "1v5 Ace Moment", engagementScore: 98, reason: "Ace detected + max chat activity", game: "Valorant" },
  ];

  await sendCallback({ jobId: job.jobId, status: "completed", progressPct: 100, step: "Complete", clips: mockClips });
  console.log(`[Worker] Completed job ${job.jobId} with ${mockClips.length} clips`);
}

async function pollQueue() {
  try {
    const result = await redis.rpop(QUEUE_KEY);
    if (result) {
      const job = typeof result === "string" ? JSON.parse(result) : result;
      await processJob(job);
    }
  } catch (error) {
    console.error("[Worker] Error polling queue:", error);
  }
  setTimeout(pollQueue, POLL_INTERVAL);
}

console.log("[Worker] EZClips worker started");
console.log(`[Worker] Polling queue every ${POLL_INTERVAL / 1000}s`);
console.log(`[Worker] API URL: ${API_URL}`);

pollQueue();
