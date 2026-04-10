'use strict';

const { v4: uuidv4 }            = require('uuid');
const { enqueueImageUploadJob } = require('./queues/image-upload.queue');
const { COLLECTION_MAP }        = require('./config/collections');
const ImageUploadJob            = require('./models/image-upload-job.model');
const mongoose                  = require('mongoose');
const logger                    = require('../../utils/logger');

/* ─────────────────────────────────────────────
   POST /image-upload/start
───────────────────────────────────────────── */
async function startImageUpload(req, res) {
  try {
    const jobId = uuidv4();

    const collections = COLLECTION_MAP.map(({ collection, s3Folder }) => ({
      collection, s3Folder,
      total: 0, processed: 0, success: 0, skipped: 0, partial: 0, failed: 0,
      status: 'pending',
    }));

    await ImageUploadJob.create({ jobId, collections });
    await enqueueImageUploadJob(jobId);

    logger.info(`[img-upload] Job created and queued: ${jobId}`);
    return res.status(202).json({
      success: true,
      jobId,
      message: 'Image upload job queued. Poll /image-upload/status/:jobId for progress.',
    });
  } catch (err) {
    logger.error(`[img-upload] startImageUpload error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────
   GET /image-upload/status/:jobId
───────────────────────────────────────────── */
async function getImageUploadStatus(req, res) {
  try {
    const job = await ImageUploadJob.findOne({ jobId: req.params.jobId }).lean();
    if (!job) return res.status(404).json({ success: false, error: 'Job not found' });

    return res.json({ success: true, job });
  } catch (err) {
    logger.error(`[img-upload] getImageUploadStatus error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────
   POST /image-upload/cancel/:jobId
───────────────────────────────────────────── */
async function cancelImageUpload(req, res) {
  try {
    const result = await ImageUploadJob.updateOne(
      { jobId: req.params.jobId, status: { $in: ['pending', 'running'] } },
      { $set: { is_cancelled: true } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: 'Job not found or already completed/cancelled',
      });
    }

    logger.info(`[img-upload] Job cancel requested: ${req.params.jobId}`);
    return res.json({ success: true, message: 'Cancellation requested' });
  } catch (err) {
    logger.error(`[img-upload] cancelImageUpload error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────
   GET /image-upload/jobs
───────────────────────────────────────────── */
async function listImageUploadJobs(req, res) {
  try {
    const jobs = await ImageUploadJob.find()
      .sort({ createdAt: -1 })
      .limit(20)
      .select('jobId status total_docs total_processed total_success total_skipped total_partial total_failed started_at finished_at createdAt')
      .lean();

    return res.json({ success: true, jobs });
  } catch (err) {
    logger.error(`[img-upload] listImageUploadJobs error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────
   POST /image-upload/rollback/:collection
   Restores image_url = image_url_old for all migrated docs.
   Optional body: { "certNum": "123" } to rollback single record.
───────────────────────────────────────────── */
async function rollbackCollection(req, res) {
  const { collection } = req.params;
  const { certNum }    = req.body || {};

  const valid = COLLECTION_MAP.find((c) => c.collection === collection);
  if (!valid) {
    return res.status(400).json({
      success: false,
      error: `Unknown collection: ${collection}. Valid: ${COLLECTION_MAP.map((c) => c.collection).join(', ')}`,
    });
  }

  try {
    const db  = mongoose.connection.db;
    const col = db.collection(collection);

    const filter = certNum
      ? { image_url_old: { $exists: true, $ne: null }, certificate_num: certNum }
      : { image_url_old: { $exists: true, $ne: null } };

    const total = await col.countDocuments(filter);
    logger.info(`[img-upload][rollback][${collection}] Docs to rollback: ${total}`);

    if (total === 0) {
      return res.json({ success: true, message: 'No migrated records found', rolled_back: 0 });
    }

    let rolledBack = 0;
    let failed     = 0;
    const cursor   = col.find(filter).batchSize(500);

    for await (const doc of cursor) {
      try {
        await col.updateOne(
          { _id: doc._id },
          {
            $set:   { image_url: doc.image_url_old },
            $unset: { image_url_old: '', main_image_s3: '', has_img: '' },
          }
        );
        rolledBack++;
      } catch (err) {
        logger.error(`[img-upload][rollback] Failed for ${doc._id}: ${err.message}`);
        failed++;
      }
    }

    logger.info(`[img-upload][rollback][${collection}] Done — rolled back: ${rolledBack}, failed: ${failed}`);
    return res.json({ success: true, collection, total, rolled_back: rolledBack, failed });

  } catch (err) {
    logger.error(`[img-upload] rollbackCollection error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  startImageUpload,
  getImageUploadStatus,
  cancelImageUpload,
  listImageUploadJobs,
  rollbackCollection,
};