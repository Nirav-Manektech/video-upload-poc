const express = require("express");
const multer = require("multer");
const path = require("path");
const { Kafka } = require("kafkajs");
const fs = require("fs");
const { exec } = require("child_process");

// Kafka setup
const kafka = new Kafka({
  clientId: "video-transcoder",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

// Initialize Express app
const app = express();
const port = 3000;

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

// Middleware to handle JSON requests
app.use(express.json());
app.use("/uploads", express.static("uploads")); // Serve uploaded files

// POST route to trigger transcoding (Producer) with file upload
app.post("/upload", upload.single("video"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ message: "No video file uploaded." });
  }

  const videoPath = path.join(__dirname, "upload", req.file.filename);
  const jobId = Date.now().toString();

  // Send transcoding job to Kafka
  await producer.send({
    topic: "video-transcode-topic",
    messages: [{ value: JSON.stringify({ videoPath: req.file.path, jobId }) }],
  });

  console.log("Job sent to Kafka:", jobId);

  res.status(200).json({ message: "Video transcoding job has been queued." });
});

// Initialize Kafka producer and start server
async function startServer() {
  await producer.connect();
  app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
  });
}

startServer().catch(console.error);
