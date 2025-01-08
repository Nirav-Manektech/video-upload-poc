const { Kafka, logLevel } = require("kafkajs");
const path = require("path");
const fs = require("fs");
const { exec } = require("child_process");

// Kafka setup
const kafka = new Kafka({
  clientId: "video-transcoder",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({
  groupId: "video-transcoder-group",
  logLevel: logLevel.DEBUG,
});

// Transcoding function (same as before)
async function transcodeVideo(inputPath, jobId) {
  console.log("Transcodeing started ---->", new Date());
  const resolutions = [
    { height: 144, bitrate: "300k" },
    { height: 360, bitrate: "500k" },
    { height: 720, bitrate: "1000k" },
    { height: 1080, bitrate: "3000k" },
  ];
  const outputDir = path.join(__dirname, "output", jobId);
  fs.mkdirSync(outputDir, { recursive: true });

  const totalResolutions = resolutions.length;
  let completedResolutions = 0;

  await Promise.all(
    resolutions.map((res) => {
      return new Promise((resolve, reject) => {
        const playlistPath = path.join(outputDir, `${res.height}p.m3u8`);
        const segmentFilename = path.join(outputDir, `${res.height}p_%03d.ts`);

        const cmd = `ffmpeg -i "${inputPath}" -vf "scale=-2:${res.height}" -c:v libx264 -b:v ${res.bitrate} -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${segmentFilename}" "${playlistPath}"`;

        exec(cmd, (err) => {
          if (err) {
            console.error(`Error transcoding ${res.height}p: ${err.message}`);
            reject(err);
          } else {
            completedResolutions++;
            const progress = Math.round(
              (completedResolutions / totalResolutions) * 100
            );
            console.log(`Progress: ${res.height} ---> ${progress}%`);
            resolve();
          }
        });
      });
    })
  );

  const masterPlaylistPath = path.join(outputDir, "master.m3u8");
  const variantStreams = resolutions
    .map(
      (res) =>
        `#EXT-X-STREAM-INF:BANDWIDTH=${
          parseInt(res.bitrate) * 1000
        },RESOLUTION=-2x${res.height}\n${res.height}p.m3u8`
    )
    .join("\n");

  fs.writeFileSync(masterPlaylistPath, `#EXTM3U\n${variantStreams}`, "utf8");
  console.log("Master playlist created at:", masterPlaylistPath);
  return { outputDir, masterPlaylist: masterPlaylistPath };
}

// Initialize Kafka consumer
async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "video-transcode-topic",
    fromBeginning: true,
  });

  // Listen for messages and process video transcoding
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { videoPath, jobId } = JSON.parse(message.value.toString());
      console.log(`Processing video job ${jobId}...,${videoPath}`);

      try {
        // Call the transcode function
        await transcodeVideo(videoPath, jobId);
        console.log(`Job ${jobId} completed`);
        console.log("Transcodeed finished", new Date());
      } catch (error) {
        console.error("Error processing video job:", error);
        // Optionally, handle retry logic or send the job to a dead-letter queue
      }
    },
  });
}

startConsumer().catch(console.error);
