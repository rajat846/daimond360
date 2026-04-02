require("node:dns/promises").setServers(["1.1.1.1", "8.8.8.8"]);
'use strict';

require('dotenv').config();

const { Worker } = require('bullmq');
const mongoose   = require('mongoose');

const { QUEUE_NAME, redisConnection }  = require('../queues/queue');
const { parseCSV }                     = require('../services/csv.service');
const { fetchVideoData }               = require('../services/video.service');
const { extractFrames }                = require('../services/frame.service');
const { generateStrips }               = require('../services/strip.service');
const { uploadStripsToS3, uploadMainImageToS3 } = require('../services/s3.service');
const { cleanupCertificate, cleanupUploadedCSV } = require('../services/cleanup.service');
const Diamond = require('../models/diamond.model');
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

  if (!diamond || diamond.processing_status !== 'completed') return false;

  // completed means all processing finished successfully — safe to skip
  return true;
}

/* ─────────────────────────────────────────────────────────────
   Core row processor

   Two paths:
   A) has video  → download frames, generate strips, upload strips
   B) no video   → skip strip generation entirely
   Both paths upload main image (if present) and save to DB.
   NOTHING here throws to the caller — all failures are handled
   internally and reflected in the DB record.
───────────────────────────────────────────────────────────── */

async function processRow(row, jobId) {
  const cert    = row.certificate_num;
  const stockId = row.stock_num || '';  // stock_num may be absent in some CSVs

  logger.info(`[${jobId}] [${cert}] BEGIN`);

  // ── 1. Fetch video data — returns null if no 360 video exists ──
  const v360     = await fetchVideoData(cert);
  const hasVideo = v360 !== null;

  let stripUrls  = [];
  let stripMeta  = null;

  if (hasVideo) {
    logger.info(`[${cert}] has_video=true — frames: ${v360.frame_count}`);

    // ── 2. Download all frames ──────────────────────────────────
    const framesDir = await extractFrames(cert, v360.url, v360.frame_count);

    // ── 3. Generate 10 WebP strips ─────────────────────────────
    const { stripPaths, meta } = await generateStrips(cert, framesDir, v360.frame_count);
    stripMeta = meta;

    // ── 4. Upload strips to S3 ─────────────────────────────────
    stripUrls = await uploadStripsToS3(cert, stripPaths);
  } else {
    logger.info(`[${cert}] has_video=false — skipping strip generation`);
  }

  // ── 5. Upload CSV image (certNumber folder) ───────────────────
  // Always attempted regardless of video presence.
  // uploadMainImageToS3 never throws — returns '' on any failure.
  const mainImageS3 = await uploadMainImageToS3(cert, row.image_url);

  // ── 6. Compute l/w ratio — length ÷ WIDTH (not depth) ────────
  //   l_w_ratio = length / width. Both must be positive numbers.
  const lwRatio =
    row.length && row.width && row.length > 0 && row.width > 0
      ? Number((row.length / row.width).toFixed(2))
      : null;

  // ── 7. Upsert diamond document ────────────────────────────────
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
───────────────────────────────────────────────────────────── */

async function handleJob(bullJob) {
  const { jobId, filePath, filename } = bullJob.data;
  logger.info(`Worker picked up job: ${jobId} (file: ${filename})`);

  await Job.findOneAndUpdate({ jobId }, { status: 'processing', started_at: new Date() });

  const { rows } = await parseCSV(filePath);

  await Job.findOneAndUpdate({ jobId }, { total_rows: rows.length });
  logger.info(`[${jobId}] Total rows: ${rows.length}`);

  for (const row of rows) {
    // Cancellation check before every row
    if (await isCancelled(jobId)) {
      logger.info(`[${jobId}] Cancelled at row ${row._rowIndex}/${rows.length}`);
      await Job.findOneAndUpdate(
        { jobId },
        { status: 'cancelled', completed_at: new Date(), cancelled_at: new Date() }
      );
      await cleanupUploadedCSV(filePath);
      return;
    }

    const cert = row.certificate_num;
    const idx  = row._rowIndex;

    logger.info(`[${jobId}] Processing ${idx} / ${rows.length}  (cert: ${cert})`);

    // Skip if already fully processed
    if (await isAlreadyProcessed(cert)) {
      logger.info(`[${jobId}] [${cert}] Already completed — skipping`);
      await Job.findOneAndUpdate({ jobId }, { $inc: { processed: 1, succeeded: 1, skipped: 1 } });
      continue;
    }

    try {
      await processRow(row, jobId);
      await Job.findOneAndUpdate({ jobId }, { $inc: { processed: 1, succeeded: 1 } });
    } catch (err) {
      logger.error(`[${jobId}] [${cert}] Row ${idx} FAILED: ${err.message}`);

      // Save failure state — include stock_num to avoid required-field validation
      // errors when this is the first time we've seen this certificate.
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
      // Always clean temp files — even on error
      await cleanupCertificate(cert);
    }
  }

  await Job.findOneAndUpdate({ jobId }, { status: 'completed', completed_at: new Date() });
  await cleanupUploadedCSV(filePath);
  logger.info(`[${jobId}] Job complete — ${rows.length} rows`);
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