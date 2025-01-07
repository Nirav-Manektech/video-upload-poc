const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { Queue: QueueMQ, Worker } = require("bullmq");
const express = require("express");

// Helper function to simulate async task (sleep)
const sleep = (t) => new Promise((resolve) => setTimeout(resolve, t * 1000));

// Redis connection settings
const redisOptions = {
  port: 6379,
  host: "localhost",
  password: "",
  tls: false,
};

// Create a BullMQ queue instance
const createQueueMQ = (name) => new QueueMQ(name, { connection: redisOptions });

// Set up BullMQ Worker
function setupBullMQProcessor(queueName) {
  new Worker(
    queueName,
    async (job) => {
      for (let i = 0; i <= 100; i++) {
        await sleep(Math.random());
        await job.updateProgress(i);
        await job.log(`Processing job at interval ${i}`);

        // Simulate a random error in job processing
        if (Math.random() * 200 < 1) throw new Error(`Random error ${i}`);
      }

      return { jobId: `This is the return value of job (${job.id})` };
    },
    { connection: redisOptions }
  );
}

const run = async () => {
  const exampleBullMq = createQueueMQ("BullMQ"); // Create queue with the name "BullMQ"

  // Setup the worker for this queue
  await setupBullMQProcessor(exampleBullMq.name);

  const app = express();

  // Create a server adapter for Bull Board
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/ui"); // The base path for the UI

  // Set up Bull Board
  createBullBoard({
    queues: [new BullMQAdapter(exampleBullMq)], // Register the queue in Bull Board
    serverAdapter,
  });

  // Route to serve the Bull Board UI
  app.use("/ui", serverAdapter.getRouter());

  // API route to add jobs to the queue
  app.use("/add", (req, res) => {
    const opts = req.query.opts || {};

    if (opts.delay) {
      opts.delay = +opts.delay * 1000; // Delay must be a number
    }

    // Add job to the queue
    exampleBullMq.add("Add", { title: req.query.title }, opts);

    res.json({
      ok: true,
    });
  });

  // Start the Express server
  app.listen(3004, () => {
    console.log("Running on 3000...");
    console.log("For the UI, open http://localhost:3000/ui");
    console.log("Make sure Redis is running on port 6379 by default");
    console.log("To populate the queue, run:");
    console.log("  curl http://localhost:3000/add?title=Example");
    console.log("To populate the queue with custom options (opts), run:");
    console.log("  curl http://localhost:3000/add?title=Test&opts[delay]=9");
  });
};

// Run the server
run().catch((e) => console.error(e));
