const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { Queue: QueueMQ, Worker } = require("bullmq");
const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { exec } = require("child_process");

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

// async function transcodeVideo(inputPath, countDuration) {
//   const resolutions = [144, 360, 720, 1080]; // Example resolutions
//   const startTime = Date.now();
//   let completed = 0;

//   for (const res of resolutions) {
//     const outputPath = path.join(__dirname, "output", `video-${res}p.mp4`);
//     exec(`ffmpeg -i ${inputPath} -vf scale=-1:${res} ${outputPath}`, (err) => {
//       if (err) console.error(`Error transcoding: ${err.message}`);
//       else {
//         console.log(`Transcoded to ${res}p`);
//         completed++;
//         console.log("Completed " + completed);

//         if (completed === resolutions.length) {
//           const endTime = Date.now();
//           const duration = (endTime - startTime) / 1000;
//           console.log("Duration counted", duration);
//           countDuration(duration);
//         }
//       }
//     });
//   }
// }
// function setupBullMQProcessor2(queueName) {
//   new Worker(
//     queueName,
//     async (job) => {
//       console.log("in worker job started", job.data.videoPath);
//       const inputPath = path.join(
//         __dirname,
//         "upload",
//         job.data.videoPath.split("\\")[1]
//       );
//       console.log(`Processing job: ${job.id} \n Video-path ${inputPath}`);
//       // Call FFmpeg for transcoding
//       await transcodeVideo(inputPath, async (countDuration) => {
//         await job.updateProgress(countDuration * 33);
//         console.log(`totalDuration: ${countDuration}`);
//       });

//       // for (let i = 0; i <= 100; i++) {
//       //   await sleep(Math.random());
//       //   await job.updateProgress(i);
//       //   await job.log(`Processing job at interval ${i}`);

//       //   // Simulate a random error in job processing
//       //   if (Math.random() * 200 < 1) throw new Error(`Random error ${i}`);
//       // }

//       // return { jobId: `This is the return value of job (${job.id})` };
//     },
//     { connection: redisOptions }
//   );
// }

async function transcodeVideo(job, inputPath, jobId) {
  const resolutions = [144, 360, 720, 1080]; // Example resolutions
  const startTime = Date.now();
  let completed = 0;
  const totalResolutions = resolutions.length;

  // Transcode each resolution
  await Promise.all(
    resolutions.map((res) => {
      return new Promise((resolve, reject) => {
        const jobOutputDir = path.join(__dirname, "output", jobId);
        fs.mkdirSync(jobOutputDir, { recursive: true });
        const outputPath = path.join(
          __dirname,
          `output/${jobId}`,
          `video-${res}p.mp4`
        );
        exec(
          `ffmpeg -i ${inputPath} -vf scale=-1:${res} ${outputPath}`,
          async (err) => {
            if (err) {
              console.error(`Error transcoding ${res}p: ${err.message}`);
              reject(err);
            } else {
              completed++;
              const progress = Math.floor((completed / totalResolutions) * 100);
              await job.updateProgress(progress); // Update progress in real-time
              console.log(`Transcoded to ${res}p, Progress: ${progress}%`);
              resolve();
            }
          }
        );
      });
    })
  );

  // Calculate duration once all resolutions are done
  const endTime = Date.now();
  const duration = (endTime - startTime) / 1000;
  console.log(`All transcoding completed in ${duration} seconds`);

  return { duration, resolutions: totalResolutions };
}

function setupBullMQProcessor2(queueName) {
  new Worker(
    queueName,
    async (job) => {
      console.log("Processing job started", job.data.videoPath);
      const inputPath = path.join(
        __dirname,
        "upload",
        job.data.videoPath.split("\\")[1]
      );
      console.log(`Processing job: ${job.id} \n Video-path ${inputPath}`);

      // Call transcodeVideo and pass the job for real-time updates
      const result = await transcodeVideo(job, inputPath, job.id);

      console.log("Job completed with result:", result);

      // Return the result to mark job as complete
      return result;
    },
    { connection: redisOptions }
  );
}

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
  const exampleBullMq = createQueueMQ("BullMQ"); // Create queue with the name "BullMQ"
  // console.log("videoPath", req.file);
  // const videoPath = req.file.path; // Path to the uploaded video
  const DummyMq = createQueueMQ("BullMQDummy"); // Create queue with the name "BullMQ"

  // Setup the worker for this queue
  await setupBullMQProcessor(DummyMq.name);
  // const job = await videoQueue.add("transcode", { videoPath });
  await setupBullMQProcessor2(exampleBullMq.name);
  const app = express();

  // Create a server adapter for Bull Board
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath("/ui"); // The base path for the UI

  // Set up Bull Board
  createBullBoard({
    queues: [new BullMQAdapter(exampleBullMq), new BullMQAdapter(DummyMq)], // Register the queue in Bull Board
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
    DummyMq.add("Add", { title: req.query.title }, opts);

    res.json({
      ok: true,
    });
  });

  app.post("/upload", upload.single("video"), async (req, res) => {
    try {
      console.log("videoPath", req.file);
      const videoPath = req.file.path; // Path to the uploaded video
      // const VideoUploadMQ = createQueueMQ("VideoUpload", { videoPath }); // Create queue with the name "BullMQ"

      // Setup the worker for this queue
      // await setupBullMQProcessor(exampleBullMq.name);
      // const job = await videoQueue.add("transcode", { videoPath });
      // await setupBullMQProcessor2(VideoUploadMQ.name);
      const opts = req.query.opts || {};

      if (opts.delay) {
        opts.delay = +opts.delay * 1000; // Delay must be a number
      }
      exampleBullMq.add("Add", { title: "Video-Upload", videoPath }, opts);
      res.status(200).send({
        message: "Video uploaded and processing started.",
        videoPath,
        jobId: exampleBullMq.name,
      });
    } catch (err) {
      console.error("Error uploading video:", err);
      res.status(500).send({ error: "Failed to upload video." });
    }
  });

  // Start the Express server
  app.listen(3004, () => {
    console.log("Running on 3004...");
    console.log("For the UI, open http://localhost:3004/ui");
    console.log("Make sure Redis is running on port 6379 by default");
    console.log("To populate the queue, run:");
    console.log("  curl http://localhost:3000/add?title=Example");
    console.log("To populate the queue with custom options (opts), run:");
    console.log("  curl http://localhost:3000/add?title=Test&opts[delay]=9");
  });
};

// Run the server
run().catch((e) => console.error(e));
