'use strict';

const { v4: uuidv4 }  = require('uuid');
const { enqueueCSVJob, getQueue } = require('../queues/queue');
const Job    = require('../models/job.model');
const logger = require('../utils/logger');

/* ─────────────────────────────────────────────────────────────
   POST /upload-csv
───────────────────────────────────────────────────────────── */

async function uploadCSV(req, res) {
  try {
    if (!req.file) {
      return res.status(400).json({
        success: false,
        error: 'No CSV file uploaded. Use field name "csv".',
      });
    }

    const jobId    = uuidv4();
    const filePath = req.file.path;
    const filename = req.file.originalname;

    // Persist job record before enqueue so status endpoint always finds it
    await Job.create({ jobId, filename, status: 'queued', total_rows: 0 });

    // Enqueue background job — returns immediately
    await enqueueCSVJob(jobId, filePath, filename);

    logger.info(`Job created: ${jobId} for file: ${filename}`);

    return res.status(202).json({
      success:   true,
      message:   'Processing started',
      jobId,
      statusUrl: `/job-status/${jobId}`,
      cancelUrl: `/job-cancel/${jobId}`,
    });
  } catch (err) {
    logger.error(`uploadCSV error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────────────────────
   GET /job-status/:id
───────────────────────────────────────────────────────────── */

async function getJobStatus(req, res) {
  try {
    const { id } = req.params;
    const job    = await Job.findOne({ jobId: id }).lean();

    if (!job) {
      return res.status(404).json({ success: false, error: `Job not found: ${id}` });
    }

    const progress =
      job.total_rows > 0
        ? Math.round((job.processed / job.total_rows) * 100)
        : 0;

    return res.json({
      success:      true,
      jobId:        job.jobId,
      status:       job.status,
      is_cancelled: job.is_cancelled,
      filename:     job.filename,
      total_rows:   job.total_rows,
      processed:    job.processed,
      succeeded:    job.succeeded,
      failed:       job.failed,
      skipped:      job.skipped,
      progress_pct: progress,
      errors:       job.row_errors,
      started_at:   job.started_at,
      completed_at: job.completed_at,
      cancelled_at: job.cancelled_at,
      created_at:   job.createdAt,
    });
  } catch (err) {
    logger.error(`getJobStatus error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/* ─────────────────────────────────────────────────────────────
   POST /job-cancel/:id

   Sets is_cancelled = true on the Job document.
   The worker reads this flag after each row and exits the loop cleanly.

   Two cases handled:
   1. Job is queued (not yet picked up) — mark cancelled immediately,
      also remove it from the BullMQ queue so the worker never starts it.
   2. Job is processing — set the flag; worker stops after current row.
───────────────────────────────────────────────────────────── */

async function cancelJob(req, res) {
  try {
    const { id } = req.params;
    const job    = await Job.findOne({ jobId: id }).lean();

    if (!job) {
      return res.status(404).json({ success: false, error: `Job not found: ${id}` });
    }

    if (['completed', 'failed', 'cancelled'].includes(job.status)) {
      return res.status(409).json({
        success: false,
        error:   `Cannot cancel a job that is already ${job.status}`,
      });
    }

    // Set the cancellation flag — worker checks this after every row
    await Job.findOneAndUpdate(
      { jobId: id },
      {
        is_cancelled: true,
        // If still queued (not yet started) mark it cancelled right now
        ...(job.status === 'queued' && {
          status:       'cancelled',
          completed_at: new Date(),
          cancelled_at: new Date(),
        }),
      }
    );

    // If the job is still sitting in the BullMQ queue (not picked up yet),
    // remove it so the worker never starts it.
    if (job.status === 'queued') {
      try {
        const queue   = getQueue();
        const bullJob = await queue.getJob(id);
        if (bullJob) await bullJob.remove();
        logger.info(`BullMQ job ${id} removed from queue`);
      } catch (qErr) {
        // Non-fatal — job may have just been picked up
        logger.warn(`Could not remove BullMQ job ${id}: ${qErr.message}`);
      }
    }

    logger.info(`Job ${id} cancellation requested (was: ${job.status})`);

    return res.json({
      success: true,
      message:
        job.status === 'queued'
          ? 'Job cancelled — it had not started yet'
          : 'Cancellation requested — job will stop after the current row finishes',
      jobId:  id,
      status: job.status === 'queued' ? 'cancelled' : 'cancelling',
    });
  } catch (err) {
    logger.error(`cancelJob error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = { uploadCSV, getJobStatus, cancelJob };