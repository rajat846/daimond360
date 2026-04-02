'use strict';

const { Queue } = require('bullmq');
const logger    = require('../utils/logger');

const QUEUE_NAME = 'diamond360-processing';

/** Shared Redis connection options */
const redisConnection = {
  host:     process.env.REDIS_HOST     || '127.0.0.1',
  port:     parseInt(process.env.REDIS_PORT, 10) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
};

/** Singleton queue instance */
let _queue = null;

function getQueue() {
  if (!_queue) {
    _queue = new Queue(QUEUE_NAME, {
      connection: redisConnection,
      defaultJobOptions: {
        attempts:    1,            // We handle retries inside the worker
        removeOnComplete: { count: 100 },
        removeOnFail:     { count: 200 },
      },
    });

    _queue.on('error', (err) => {
      logger.error(`BullMQ queue error: ${err.message}`);
    });
  }
  return _queue;
}

/**
 * Enqueues a CSV processing job.
 *
 * @param {string} jobId    - Unique job ID (also stored in DB)
 * @param {string} filePath - Absolute path to the uploaded CSV
 * @param {string} filename - Original filename (for display)
 * @returns {Promise<Job>}
 */
async function enqueueCSVJob(jobId, filePath, filename) {
  const queue = getQueue();
  const job   = await queue.add(
    'process-csv',
    { jobId, filePath, filename },
    { jobId } // Use our own jobId as BullMQ job id for easy lookup
  );
  logger.info(`Job enqueued: ${jobId} (file: ${filename})`);
  return job;
}

module.exports = { getQueue, enqueueCSVJob, QUEUE_NAME, redisConnection };
