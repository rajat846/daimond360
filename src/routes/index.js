'use strict';

const router = require('express').Router();
const upload = require('../middleware/upload.middleware');
const { uploadCSV, getJobStatus, cancelJob } = require('../controllers/upload.controller');
const { listProducts, getProduct }           = require('../controllers/product.controller');

// POST /upload-csv  — upload CSV, start background job
router.post('/upload-csv', upload.single('csv'), uploadCSV);

// GET  /job-status/:id  — poll progress
router.get('/job-status/:id', getJobStatus);

// POST /job-cancel/:id  — stop an ongoing or queued job
router.post('/job-cancel/:id', cancelJob);

// GET  /api/products        — paginated product list
router.get('/api/products', listProducts);

// GET  /api/products/:certificateNum  — single product
router.get('/api/products/:certificateNum', getProduct);

// GET  /health
router.get('/health', (_req, res) => {
  res.json({ status: 'ok', ts: new Date().toISOString() });
});

module.exports = router;