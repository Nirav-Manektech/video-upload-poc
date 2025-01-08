const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { Queue: QueueMQ } = require("bullmq");
const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");

// Redis connection settings
const redisOptions = {
  port: 6379,
  host: "localhost",
  password: "",
  tls: false,
};

// Create a BullMQ queue instance
const createQueueMQ = (name) => new QueueMQ(name, { connection: redisOptions });

// Configure Multer storage for video upload
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "upload/"); // Directory to store uploaded files
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(
      null,
      `${file.fieldname}-${uniqueSuffix}${path.extname(file.originalname)}`
    );
  },
});
const upload = multer({ storage });

const run = async () => {
  const videoQueue = createQueueMQ("video-transcode-queue"); // Main queue

  const app = express();

  // Set up Bull Board
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/ui");

  createBullBoard({
    queues: [new BullMQAdapter(videoQueue)],
    serverAdapter,
  });

  app.use("/ui", serverAdapter.getRouter());

  // Upload route
  app.post("/upload", upload.single("video"), async (req, res) => {
    try {
      console.log("Uploaded file path:", req.file);
      const videoPath = req.file.path; // Path to the uploaded video

      // Add job to the queue
      const job = await videoQueue.add("transcode", { videoPath });
      res.status(200).send({
        message: "Video uploaded and processing started.",
        videoPath,
        jobId: job.id,
      });
    } catch (err) {
      console.error("Error uploading video:", err);
      res.status(500).send({ error: "Failed to upload video." });
    }
  });

  // Start the server
  app.listen(3004, () => {
    console.log("Running on 3004...");
    console.log("For the UI, open http://localhost:3004/ui");
  });
};

run().catch((e) => console.error(e));
