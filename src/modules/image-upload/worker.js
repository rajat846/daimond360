require('node:dns/promises').setServers(['1.1.1.1', '8.8.8.8']);
'use strict';

require('dotenv').config();

const { Worker }  = require('bullmq');
const mongoose    = require('mongoose');

const { IMAGE_UPLOAD_QUEUE, redisConnection } = require('./queues/image-upload.queue');
const { COLLECTION_MAP, buildS3Key }          = require('./config/collections');
const { downloadCompressUpload }              = require('./services/image-s3.service');
const ImageUploadJob                          = require('./models/image-upload-job.model');
const logger                                  = require('../../utils/logger');

const BATCH_SIZE  = 500;
const CONCURRENCY = 20;

/* ─────────────────────────────────────────────
   DB
───────────────────────────────────────────── */
async function connectDB() {
  await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/diamond360');
  logger.info('[img-migrate] Worker connected to MongoDB');
}

/* ─────────────────────────────────────────────
   Cancellation check
───────────────────────────────────────────── */
async function isCancelled(jobId) {
  const job = await ImageUploadJob.findOne({ jobId }, { is_cancelled: 1 }).lean();
  return job?.is_cancelled === true;
}

/* ─────────────────────────────────────────────
   Filter: docs eligible for migration
   - has image_url
   - image_url_old does NOT exist (not yet migrated)
───────────────────────────────────────────── */
function eligibleFilter() {
  return {
    image_url:     { $exists: true, $ne: null, $nin: ['', null] },
    image_url_old: { $exists: false },
  };
}

/* ─────────────────────────────────────────────
   Classify doc before processing

   skip    — image_url_old exists → already migrated
   partial — image_url_old exists but image_url missing → do NOT touch
   migrate — normal, eligible for migration
   no_url  — no image_url at all → nothing to do
───────────────────────────────────────────── */
function classifyDoc(doc) {
  const hasOld = doc.image_url_old && doc.image_url_old.trim() !== '';
  const hasNew = doc.image_url     && doc.image_url.trim()     !== '';

  if (hasOld && hasNew)  return 'skip';
  if (hasOld && !hasNew) return 'partial';
  if (!hasOld && hasNew) return 'migrate';
  return 'no_url';
}

/* ─────────────────────────────────────────────
   Process one document
───────────────────────────────────────────── */
async function processDoc(doc, s3Folder, collectionName) {
  const certNum = doc.certificate_num || doc.cert_num || doc.certNo || String(doc._id);
  const kind    = classifyDoc(doc);

  if (kind === 'skip') {
    logger.info(`[img-migrate][${certNum}] SKIP — already migrated`);
    return { status: 'skipped', reason: 'already_migrated', certNum };
  }

  if (kind === 'partial') {
    // image_url_old exists but image_url is gone — log for manual review, do NOT overwrite
    logger.warn(`[img-migrate][${certNum}] PARTIAL — image_url_old exists but image_url missing. Manual review needed.`);
    return { status: 'partial', reason: 'image_url_missing_after_migration', certNum };
  }

  if (kind === 'no_url') {
    logger.info(`[img-migrate][${certNum}] SKIP — no image_url`);
    return { status: 'skipped', reason: 'no_image_url', certNum };
  }

  // ── Normal migration ───────────────────────
  const originalUrl = doc.image_url.trim();
  const s3Key       = buildS3Key(s3Folder, certNum);
  const db          = mongoose.connection.db;

  try {
    const { s3Url } = await downloadCompressUpload(originalUrl, s3Key, certNum);

    // Atomic update:
    // image_url     → new compressed S3 URL
    // image_url_old → original URL (rollback key)
    // main_image_s3 → same S3 URL (keeps upload module consistent)
    await db.collection(collectionName).updateOne(
      { _id: doc._id },
      {
        $set: {
          image_url:     s3Url,
          image_url_old: originalUrl,
          main_image_s3: s3Url,
          has_img:       true,
        },
      }
    );

    logger.info(`[img-migrate][${certNum}] SUCCESS — ${originalUrl} → ${s3Url}`);
    return { status: 'success', certNum, s3Url };

  } catch (err) {
    logger.error(`[img-migrate][${certNum}] FAILED — ${err.message}`);
    return { status: 'failed', certNum, error: err.message };
  }
}

/* ─────────────────────────────────────────────
   Process one batch + persist progress
───────────────────────────────────────────── */
async function processBatch(docs, s3Folder, collectionName, jobEntry, jobId) {
  for (let i = 0; i < docs.length; i += CONCURRENCY) {
    if (await isCancelled(jobId)) break;

    const chunk   = docs.slice(i, i + CONCURRENCY);
    const results = await Promise.all(
      chunk.map((doc) => processDoc(doc, s3Folder, collectionName))
    );

    for (const r of results) {
      jobEntry.processed++;
      if      (r.status === 'success') jobEntry.success++;
      else if (r.status === 'skipped') jobEntry.skipped++;
      else if (r.status === 'partial') jobEntry.partial++;
      else                             jobEntry.failed++;
    }
  }

  await ImageUploadJob.updateOne(
    { jobId, 'collections.collection': collectionName },
    {
      $set: {
        'collections.$.processed': jobEntry.processed,
        'collections.$.success':   jobEntry.success,
        'collections.$.skipped':   jobEntry.skipped,
        'collections.$.partial':   jobEntry.partial,
        'collections.$.failed':    jobEntry.failed,
        'collections.$.total':     jobEntry.total,
        'collections.$.status':    'running',
      },
    }
  );

  logger.info(
    `[img-migrate][${collectionName}] Progress: ${jobEntry.processed}/${jobEntry.total} ` +
    `(ok:${jobEntry.success} skip:${jobEntry.skipped} partial:${jobEntry.partial} fail:${jobEntry.failed})`
  );
}

/* ─────────────────────────────────────────────
   Process entire collection via cursor
───────────────────────────────────────────── */
async function processCollection(collectionName, s3Folder, jobEntry, jobId) {
  const db     = mongoose.connection.db;
  const col    = db.collection(collectionName);
  const filter = eligibleFilter();

  const total     = Math.min(await col.countDocuments(filter), 1000);
  jobEntry.total  = total;
  jobEntry.status = 'running';
  logger.info(`[img-migrate][${collectionName}] Eligible docs: ${total}`);

  if (total === 0) {
    logger.info(`[img-migrate][${collectionName}] Nothing to migrate`);
    jobEntry.status = 'completed';
    await ImageUploadJob.updateOne(
      { jobId, 'collections.collection': collectionName },
      { $set: { 'collections.$.status': 'completed', 'collections.$.total': 0 } }
    );
    return;
  }

  const cursor = col.find(filter).limit(1000).batchSize(BATCH_SIZE);
  let batch    = [];

  for await (const doc of cursor) {
    if (await isCancelled(jobId)) {
      logger.info(`[img-migrate][${collectionName}] Cancelled — closing cursor`);
      await cursor.close();
      break;
    }

    batch.push(doc);

    if (batch.length === BATCH_SIZE) {
      await processBatch(batch, s3Folder, collectionName, jobEntry, jobId);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await processBatch(batch, s3Folder, collectionName, jobEntry, jobId);
  }

  jobEntry.status = 'completed';
  await ImageUploadJob.updateOne(
    { jobId, 'collections.collection': collectionName },
    { $set: { 'collections.$.status': 'completed' } }
  );
}

/* ─────────────────────────────────────────────
   Main job handler
───────────────────────────────────────────── */
async function handleJob(bullJob) {
  const { jobId } = bullJob.data;
  logger.info(`[img-migrate] Starting job: ${jobId}`);

  const jobDoc = await ImageUploadJob.findOne({ jobId });
  if (!jobDoc) throw new Error(`ImageUploadJob not found: ${jobId}`);

  jobDoc.status     = 'running';
  jobDoc.started_at = new Date();
  await jobDoc.save();

  for (const { collection, s3Folder } of COLLECTION_MAP) {
    if (await isCancelled(jobId)) {
      logger.info(`[img-migrate] Job ${jobId} cancelled before ${collection}`);
      break;
    }

    let entry = jobDoc.collections.find((c) => c.collection === collection);
    if (!entry) {
      jobDoc.collections.push({
        collection, s3Folder,
        total: 0, processed: 0, success: 0, skipped: 0, partial: 0, failed: 0,
        status: 'pending',
      });
      entry = jobDoc.collections[jobDoc.collections.length - 1];
    }

    try {
      await processCollection(collection, s3Folder, entry, jobId);
    } catch (err) {
      logger.error(`[img-migrate][${collection}] Collection error: ${err.message}`);
      entry.status = 'failed';
      await ImageUploadJob.updateOne(
        { jobId, 'collections.collection': collection },
        { $set: { 'collections.$.status': 'failed' } }
      );
    }
  }

  // Final totals
  const finalDoc = await ImageUploadJob.findOne({ jobId });
  let tp = 0, ts = 0, tsk = 0, tpa = 0, tf = 0, total = 0;
  for (const c of finalDoc.collections) {
    total += c.total;
    tp    += c.processed;
    ts    += c.success;
    tsk   += c.skipped;
    tpa   += (c.partial || 0);
    tf    += c.failed;
  }

  const cancelled = await isCancelled(jobId);
  await ImageUploadJob.updateOne(
    { jobId },
    {
      $set: {
        status:          cancelled ? 'cancelled' : 'completed',
        total_docs:      total,
        total_processed: tp,
        total_success:   ts,
        total_skipped:   tsk,
        total_partial:   tpa,
        total_failed:    tf,
        finished_at:     new Date(),
      },
    }
  );

  logger.info(
    `[img-migrate] Job ${jobId} finished — ` +
    `total:${total} ok:${ts} skip:${tsk} partial:${tpa} fail:${tf}`
  );
}

/* ─────────────────────────────────────────────
   Boot
───────────────────────────────────────────── */
async function startWorker() {
  await connectDB();

  const worker = new Worker(IMAGE_UPLOAD_QUEUE, handleJob, {
    connection:  redisConnection,
    concurrency: 1,
  });

  worker.on('completed', (job) => {
    logger.info(`[img-migrate] BullMQ job completed: ${job.id}`);
  });

  worker.on('failed', (job, err) => {
    logger.error(`[img-migrate] BullMQ job failed: ${job?.id} — ${err.message}`);
    if (job?.data?.jobId) {
      ImageUploadJob.updateOne(
        { jobId: job.data.jobId },
        { $set: { status: 'failed', error_message: err.message, finished_at: new Date() } }
      ).catch(() => {});
    }
  });

  logger.info('[img-migrate] Worker started — waiting for jobs');
}

if (require.main === module) {
  startWorker().catch((err) => {
    logger.error(`[img-migrate] Worker startup failed: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { startWorker };