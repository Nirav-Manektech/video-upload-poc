const { Worker } = require("bullmq");
const path = require("path");
const fs = require("fs");
const { spawn } = require("child_process");
const { Sema } = require("async-sema");

// Redis connection settings
const redisOptions = {
  port: 6379,
  host: "localhost",
  password: "",
  tls: false,
};

// Throttling FFmpeg processes
const maxConcurrentFFmpeg = 4; // Adjust based on system resources (e.g., number of CPU cores)
const ffmpegSemaphore = new Sema(maxConcurrentFFmpeg);

//Compressing video
async function compressVideo(inputPath, outputPath) {
  // Compress the video to a lower bitrate and resolution
  const cmd = "ffmpeg";
  const args = [
    "-i",
    inputPath,
    "-b:v",
    "1000k", // Target bitrate
    "-s",
    "1280x720", // Target resolution (720p)
    "-preset",
    "fast", // Use a faster preset for compression
    "-y",
    outputPath, // Output compressed video file
  ];

  return new Promise((resolve, reject) => {
    const process = spawn(cmd, args, { shell: true });

    process.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Compression failed with code ${code}`));
      }
    });

    process.on("error", (err) => {
      reject(err);
    });
  });
}

// Transcoding function
async function transcodeVideo(job, inputPath, jobId) {
  // Step 1: Compress the video first
  const startTime = Date.now();
  const outputDir = path.join(__dirname, "output", jobId);
  fs.mkdirSync(outputDir, { recursive: true });
  await ffmpegSemaphore.acquire(); // Throttle FFmpeg execution

  const compressedVideoPath = path.join(outputDir, "compressed_video.mp4");
  console.log(`Compressing video: ${inputPath}`);
  await compressVideo(inputPath, compressedVideoPath);
  console.log("compressed video path", compressedVideoPath);
  const cmd = "ffmpeg";
  const args = `
    -i "${compressedVideoPath}" \
    -filter_complex \
      "[0:v]split=4[v144][v360][v720][v1080]; \
       [v144]scale=w=-2:h=144[v144out]; \
       [v360]scale=w=-2:h=360[v360out]; \
       [v720]scale=w=-2:h=720[v720out]; \
       [v1080]scale=w=-2:h=1080[v1080out]" \
    -map "[v144out]" -c:v:0 libx264 -b:v:0 300k -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${outputDir}/144p_%03d.ts" "${outputDir}/144p.m3u8" \
    -map "[v360out]" -c:v:1 libx264 -b:v:1 500k -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${outputDir}/360p_%03d.ts" "${outputDir}/360p.m3u8" \
    -map "[v720out]" -c:v:2 libx264 -b:v:2 1000k -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${outputDir}/720p_%03d.ts" "${outputDir}/720p.m3u8" \
    -map "[v1080out]" -c:v:3 libx264 -b:v:3 3000k -hls_time 10 -hls_playlist_type vod -hls_segment_filename "${outputDir}/1080p_%03d.ts" "${outputDir}/1080p.m3u8"
  `
    .replace(/\s+/g, " ")
    .trim()
    .split(" ");

  try {
    return new Promise((resolve, reject) => {
      const process = spawn(cmd, args, { shell: true });

      let progress = 0;
      let duration = 0;

      process.stderr.on("data", (data) => {
        const message = data.toString();

        // Extract total duration from FFmpeg logs
        const durationMatch = message.match(/Duration: (\d+):(\d+):(\d+\.\d+)/);
        if (durationMatch) {
          const [_, hours, minutes, seconds] = durationMatch;
          duration =
            parseFloat(hours) * 3600 +
            parseFloat(minutes) * 60 +
            parseFloat(seconds);
        }

        // Extract progress from FFmpeg logs
        const timeMatch = message.match(/time=(\d+):(\d+):(\d+\.\d+)/);
        if (timeMatch) {
          const [_, hours, minutes, seconds] = timeMatch;
          const currentTime =
            parseFloat(hours) * 3600 +
            parseFloat(minutes) * 60 +
            parseFloat(seconds);

          if (duration > 0) {
            progress = Math.min(
              Math.round((currentTime / duration) * 100),
              100
            );
            job.updateProgress(progress); // Update job progress
            console.log(`Progress: ${progress}%`);
          }
        }
      });

      process.on("close", (code) => {
        if (code === 0) {
          // Generate the master.m3u8 playlist
          const masterPlaylist = `
#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=300000,RESOLUTION=256x144
144p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=640x360
360p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=1280x720
720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1920x1080
1080p.m3u8
          `;
          fs.writeFileSync(path.join(outputDir, "master.m3u8"), masterPlaylist);
          resolve();
          const endTime = Date.now();
          const duration = (endTime - startTime) / 1000;
          console.log(`totalDuration: ${duration}`);
        } else {
          reject(new Error(`FFmpeg process exited with code ${code}`));
        }
      });

      process.on("error", (err) => {
        reject(err);
      });
    });
  } finally {
    ffmpegSemaphore.release(); // Release the slot
  }
}

const worker = new Worker(
  "video-transcode-queue",
  async (job) => {
    console.log(`Processing job: ${job.id}`);
    const inputPath = job.data.videoPath;

    // Call the transcode function
    await transcodeVideo(job, inputPath, job.id);

    console.log(`Job ${job.id} completed`);
    return { status: "completed" };
  },
  {
    connection: redisOptions,
    concurrency: 4, // Allow up to 4 concurrent jobs
  }
);

worker.on("completed", (job) => {
  console.log(`Job ${job.id} is completed!`);
});

worker.on("failed", (job, err) => {
  console.error(`Job ${job.id} failed with error: ${err.message}`);
});
