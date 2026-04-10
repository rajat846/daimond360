'use strict';

const { Queue } = require('bullmq');
const logger    = require('../../../utils/logger');

const IMAGE_UPLOAD_QUEUE = 'image-upload-queue';

const redisConnection = {
  host:     process.env.REDIS_HOST     || '127.0.0.1',
  port:     parseInt(process.env.REDIS_PORT, 10) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  username: process.env.REDIS_USERNAME || 'default',
  maxRetriesPerRequest: null,
};

let _queue = null;

function getImageUploadQueue() {
  if (!_queue) {
    _queue = new Queue(IMAGE_UPLOAD_QUEUE, {
      connection: redisConnection,
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: { count: 50 },
        removeOnFail:     { count: 100 },
      },
    });

    _queue.on('error', (err) => {
      logger.error(`[img-upload] Queue error: ${err.message}`);
    });
  }
  return _queue;
}

async function enqueueImageUploadJob(jobId) {
  const queue = getImageUploadQueue();
  const job   = await queue.add('upload-images', { jobId }, { jobId });
  logger.info(`[img-upload] Job enqueued: ${jobId}`);
  return job;
}

module.exports = {
  getImageUploadQueue,
  enqueueImageUploadJob,
  IMAGE_UPLOAD_QUEUE,
  redisConnection,
};