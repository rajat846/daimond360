'use strict';

const router = require('express').Router();
const {
  startImageUpload,
  getImageUploadStatus,
  cancelImageUpload,
  listImageUploadJobs,
  rollbackCollection,
} = require('./controller');

// POST  /image-upload/start                   — trigger migration job
router.post('/start', startImageUpload);

// GET   /image-upload/status/:jobId           — poll job progress
router.get('/status/:jobId', getImageUploadStatus);

// POST  /image-upload/cancel/:jobId           — cancel running job
router.post('/cancel/:jobId', cancelImageUpload);

// GET   /image-upload/jobs                    — list recent jobs
router.get('/jobs', listImageUploadJobs);

// POST  /image-upload/rollback/:collection    — rollback entire collection
// Body (optional): { "certNum": "1513853325" } to rollback single record
router.post('/rollback/:collection', rollbackCollection);

module.exports = router;