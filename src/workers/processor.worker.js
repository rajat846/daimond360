require("node:dns/promises").setServers(["1.1.1.1", "8.8.8.8"]);
'use strict';

require('dotenv').config();

const { Worker } = require('bullmq');
const mongoose   = require('mongoose');

const { QUEUE_NAME, redisConnection }  = require('../queues/queue');
const { parseCSVStream }               = require('../services/csv.service');
const { fetchVideoData }               = require('../services/video.service');
const { extractFrames }                = require('../services/frame.service');
const { generateStrips }               = require('../services/strip.service');
const { uploadStripsToS3, uploadMainImageToS3 } = require('../services/s3.service');
const { cleanupCertificate, cleanupUploadedCSV } = require('../services/cleanup.service');
const Diamond = require('../models/diamond_v1.model');
const Job     = require('../models/job.model');
const logger  = require('../utils/logger');

/* ─────────────────────────────────────────────────────────────
   DB
───────────────────────────────────────────────────────────── */

async function connectDB() {
  await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/diamond360');
  logger.info('Worker connected to MongoDB');
}

/* ─────────────────────────────────────────────────────────────
   Cancellation check (lightweight DB poll)
───────────────────────────────────────────────────────────── */

async function isCancelled(jobId) {
  const job = await Job.findOne({ jobId }, { is_cancelled: 1 }).lean();
  return job?.is_cancelled === true;
}

/* ─────────────────────────────────────────────────────────────
   Skip-if-already-done check
───────────────────────────────────────────────────────────── */

async function isAlreadyProcessed(cert) {
  const diamond = await Diamond.findOne(
    { certificate_num: cert },
    { processing_status: 1 }
  ).lean();
  return diamond?.processing_status === 'completed';
}

/* ─────────────────────────────────────────────────────────────
   Core row processor

   CHANGE: extractFrames now returns { framesDir, usedFrameCount }
     instead of a plain path string.
   WHY: The new frame service caps frames at MAX_FRAMES and needs to
     communicate the actual count used so generateStrips receives the
     correct number (not the raw API frame_count which may be >150).
   IMPACT: Strip generation is always given the real on-disk frame count,
     so computeStripMeta() produces valid strip layouts for any input size.
   BREAK RISK: Reverting to the old single-string return from extractFrames
     would break generateStrips (wrong frame count passed in).
   FUTURE: If additional metadata is needed, extend the return object
     rather than adding positional parameters.
───────────────────────────────────────────────────────────── */

async function processRow(row, jobId, signal) {
  const cert    = row.certificate_num;
  const stockId = row.stock_num || '';

  logger.info(`[${jobId}] [${cert}] BEGIN`);

  const v360     = await fetchVideoData(cert);
  const hasVideo = v360 !== null;

  let stripUrls = [];
  let stripMeta = null;

  if (hasVideo) {
    logger.info(`[${cert}] has_video=true — frames: ${v360.frame_count}`);

    // extractFrames now accepts an AbortSignal and returns { framesDir, usedFrameCount }
    const { framesDir, usedFrameCount } = await extractFrames(
      cert, v360.url, v360.frame_count, signal
    );

    // Pass usedFrameCount (capped) not v360.frame_count (raw) so strip math
    // always operates on the actual files present on disk.
    const { stripPaths, meta } = await generateStrips(cert, framesDir, usedFrameCount);
    stripMeta = meta;

    stripUrls = await uploadStripsToS3(cert, stripPaths);
  } else {
    logger.info(`[${cert}] has_video=false — skipping strip generation`);
  }

  const mainImageS3 = await uploadMainImageToS3(cert, row.image_url);

  const lwRatio =
    row.length && row.width && row.length > 0 && row.width > 0
      ? Number((row.length / row.width).toFixed(2))
      : null;

  await Diamond.findOneAndUpdate(
    { certificate_num: cert },
    {
      $set: {
        stock_num:         stockId,
        shape:             row.shape,
        carat:             row.carat,
        clarity:           row.clarity,
        color:             row.color,
        cut:               row.cut,
        polish:            row.polish,
        symmetry:          row.symmetry,
        fluorescence:      row.fluorescence,
        depth_percent:     row.depth_percent,
        table_percent:     row.table_percent,
        length:            row.length,
        width:             row.width,
        depth:             row.depth,
        l_w_ratio:         lwRatio,
        girdle:            row.girdle,
        culet_size:        row.culet_size,
        lab:               row.lab,
        location:          row.location,
        our_price:         row.our_price,
        image_url:         row.image_url,
        main_image_s3:     mainImageS3,
        has_img:           !!(mainImageS3 && mainImageS3.trim()),
        has_video:         hasVideo,
        strips:            stripUrls,
        strip_meta:        stripMeta,
        processing_status: 'completed',
        error_message:     null,
        processed_at:      new Date(),
      },
    },
    { upsert: true, new: true }
  );

  logger.info(`[${cert}] Saved — has_video=${hasVideo}, strips=${stripUrls.length}, image=${mainImageS3 ? 'yes' : 'none'}`);
}

/* ─────────────────────────────────────────────────────────────
   BullMQ job handler

   CHANGE: Replaced parseCSV (full in-memory load) with parseCSVStream.
   WHY: parseCSV accumulated ALL rows in memory before processing began.
     For a large CSV (10k+ rows) this caused a significant peak memory
     spike at job start. Streaming processes each row as it is parsed,
     keeping memory flat across the full job duration.
   IMPACT: Job startup memory is O(1) in CSV row count instead of O(n).
     Total_rows count is reported after streaming completes.
   BREAK RISK: The Job total_rows field is now set AFTER processing
     finishes (streamed row count), not before. Progress reporting that
     relies on total_rows being set at job start must be adjusted.
   FUTURE: If pre-flight row counting is required, do a fast wc -l pass
     first rather than loading the full CSV into memory.

   CHANGE: AbortController replaces ad-hoc isCancelled polling inside
     processRow.
   WHY: Previously, cancellation could only be checked at row boundaries.
     Passing an AbortSignal lets frame downloads abort mid-stream as soon
     as a cancel is detected, freeing sockets immediately.
   IMPACT: Cancelled jobs release network resources faster.
   BREAK RISK: AbortController requires Node >= 16.14. The package.json
     already requires Node >= 18, so this is safe.
   FUTURE: Thread the signal into S3 uploads too for complete cancellation.
───────────────────────────────────────────────────────────── */

async function handleJob(bullJob) {
  const { jobId, filePath, filename } = bullJob.data;
  logger.info(`Worker picked up job: ${jobId} (file: ${filename})`);

  await Job.findOneAndUpdate({ jobId }, { status: 'processing', started_at: new Date() });

  // AbortController for cooperative cancellation across async boundaries
  const abortController = new AbortController();
  const { signal } = abortController;

  let cancelled = false;
  let totalRows = 0;

  try {
    // Stream CSV rows — never loads the full file into memory
    await parseCSVStream(filePath, async (row) => {
      // If we already detected a cancel, stop processing additional rows
      if (cancelled) return;

      totalRows++;
      const cert = row.certificate_num;
      const idx  = row._rowIndex;

      // Cancellation check before each row — abort in-flight downloads too
      if (await isCancelled(jobId)) {
        logger.info(`[${jobId}] Cancelled at row ${idx}`);
        cancelled = true;
        abortController.abort();

        await Job.findOneAndUpdate(
          { jobId },
          { status: 'cancelled', completed_at: new Date(), cancelled_at: new Date() }
        );
        await cleanupUploadedCSV(filePath);
        return;
      }

      logger.info(`[${jobId}] Processing row ${idx} (cert: ${cert})`);

      if (await isAlreadyProcessed(cert)) {
        logger.info(`[${jobId}] [${cert}] Already completed — skipping`);
        await Job.findOneAndUpdate({ jobId }, { $inc: { processed: 1, succeeded: 1, skipped: 1 } });
        return;
      }

      try {
        await processRow(row, jobId, signal);
        await Job.findOneAndUpdate({ jobId }, { $inc: { processed: 1, succeeded: 1 } });
      } catch (err) {
        logger.error(`[${jobId}] [${cert}] Row ${idx} FAILED: ${err.message}`);

        await Diamond.findOneAndUpdate(
          { certificate_num: cert },
          {
            $set: {
              stock_num:         row.stock_num || '',
              processing_status: 'failed',
              error_message:     err.message,
            },
          },
          { upsert: true }
        ).catch((saveErr) => {
          logger.error(`[${cert}] Failed to save error state: ${saveErr.message}`);
        });

        await Job.findOneAndUpdate(
          { jobId },
          {
            $inc:  { processed: 1, failed: 1 },
            $push: {
              row_errors: {
                row_index:       idx,
                certificate_num: cert,
                message:         err.message,
                timestamp:       new Date(),
              },
            },
          }
        );
      } finally {
        await cleanupCertificate(cert);
      }
    });
  } finally {
    // Always update total_rows after streaming completes
    await Job.findOneAndUpdate({ jobId }, { total_rows: totalRows });
  }

  if (!cancelled) {
    await Job.findOneAndUpdate({ jobId }, { status: 'completed', completed_at: new Date() });
    await cleanupUploadedCSV(filePath);
    logger.info(`[${jobId}] Job complete — ${totalRows} rows`);
  }
}

/* ─────────────────────────────────────────────────────────────
   Worker bootstrap
───────────────────────────────────────────────────────────── */

async function startWorker() {
  await connectDB();

  const worker = new Worker(QUEUE_NAME, handleJob, {
    connection:  redisConnection,
    concurrency: 20,
  });

  worker.on('completed', (job) => logger.info(`BullMQ job ${job.id} completed`));

  worker.on('failed', async (job, err) => {
    logger.error(`BullMQ job ${job?.id} failed: ${err.message}`);
    if (job?.data?.jobId) {
      await Job.findOneAndUpdate(
        { jobId: job.data.jobId },
        { status: 'failed', completed_at: new Date() }
      ).catch(() => {});
    }
  });

  worker.on('error', (err) => logger.error(`Worker error: ${err.message}`));

  logger.info('Diamond 360 worker started — waiting for jobs...');
}

startWorker().catch((err) => {
  logger.error(`Fatal worker error: ${err.message}`);
  process.exit(1);
});