const { Worker } = require("bullmq");
const path = require("path");
const fs = require("fs");
const { exec } = require("child_process");

// Redis connection settings
const redisOptions = {
  port: 6379,
  host: "localhost",
  password: "",
  tls: false,
};

// Transcoding function
async function transcodeVideo(job, inputPath, jobId) {
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

  // Generate HLS playlists for each resolution
  await Promise.all(
    resolutions.map((res, index) => {
      return new Promise((resolve, reject) => {
        const playlistPath = path.join(outputDir, `${res.height}p.m3u8`);
        const segmentFilename = path.join(outputDir, `${res.height}p_%03d.ts`);

        const cmd = `
          ffmpeg -i "${inputPath}" \
          -vf "scale=-2:${res.height}" \
          -c:v libx264 -b:v ${res.bitrate} \
          -hls_time 10 -hls_playlist_type vod \
          -hls_segment_filename "${segmentFilename}" "${playlistPath}"
        `
          .replace(/\s+/g, " ")
          .trim();

        exec(cmd, (err) => {
          if (err) {
            console.error(`Error transcoding ${res.height}p: ${err.message}`);
            reject(err);
          } else {
            completedResolutions++;
            const progress = Math.round(
              (completedResolutions / totalResolutions) * 100
            );
            job.updateProgress(progress); // Updates job progress
            console.log(`Progress: ${progress}%`);
            resolve();
          }
        });
      });
    })
  );

  // Create the master playlist
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
