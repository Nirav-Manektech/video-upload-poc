const { Queue, Worker } = require("bullmq");
const { exec } = require("child_process");
const express = require("express");
const multer = require("multer");
const { createBullBoard } = require("@bull-board/api");
const path = require("path");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");

const app = express();

// Redis connection settings
const redisConnection = { host: "localhost", port: 6379 };

// Create a video-processing queue
const videoQueue = new Queue("video-processing", {
  connection: redisConnection,
});

// Create an ExpressAdapter instance for Bull Board
const expressAdapter = new ExpressAdapter();
expressAdapter.setQueues([videoQueue]); // Register the video processing queue for Bull Board UI

// Create the Bull Board UI at /admin/queues route
app.use("/admin/queues", expressAdapter.getRouter());

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

// Transcode video function (mock)
async function transcodeVideo(inputPath, countDuration) {
  const resolutions = [144, 360, 720, 1080]; // Example resolutions
  const startTime = Date.now();
  let completed = 0;

  for (const res of resolutions) {
    const outputPath = path.join(__dirname, "output", `video-${res}p.mp4`);
    exec(`ffmpeg -i ${inputPath} -vf scale=-1:${res} ${outputPath}`, (err) => {
      if (err) console.error(`Error transcoding: ${err.message}`);
      else {
        console.log(`Transcoded to ${res}p`);
        completed++;
        console.log("Completed " + completed);

        if (completed === resolutions.length) {
          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;
          console.log("Duration counted", duration);
          countDuration(duration);
        }
      }
    });
  }
}

// Create a worker to process video jobs
const worker = new Worker(
  "video-processing",
  async (job) => {
    const inputPath = path.join(
      __dirname,
      "upload",
      job.data.videoPath.split("\\")[1]
    );
    console.log(`Processing job: ${job.id} \n Video-path ${inputPath}`);

    // Call FFmpeg for transcoding
    await transcodeVideo(inputPath, (countDuration) => {
      console.log(`totalDuration: ${countDuration}`);
    });
  },
  { connection: redisConnection }
);

worker.on("completed", (job) => {
  console.log(`Job ${job.id} completed successfully.`);
});

worker.on("failed", (job, err) => {
  console.error(`Job ${job.id} failed: ${err.message}`);
});

// Upload video endpoint
app.post("/upload", upload.single("video"), async (req, res) => {
  try {
    console.log("videoPath", req.file);
    const videoPath = req.file.path; // Path to the uploaded video
    // Add the video processing job to the queue
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
app.listen(3002, () => {
  console.log("Server running on port 3000");
  console.log("For the UI, open http://localhost:3000/admin/queues");
  console.log("Make sure Redis is running on port 6379 by default");
});
